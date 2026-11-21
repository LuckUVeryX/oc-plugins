import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import {
  Clock,
  Config,
  Context,
  Deferred,
  Effect,
  Layer,
  Logger,
  Option,
  Schema,
  SchemaIssue,
} from "effect";
import type { JsonSchema } from "effect";
import { isObject } from "effect/Predicate";
import { decodeArguments, inputSchemaDocument } from "./arguments.ts";
import { Catalog, type RegisteredEvent } from "./catalog.ts";
import { stepTimeoutSeconds, watchDefaultSeconds } from "./config.ts";
import { Decisions, type Decide } from "./decisions.ts";
import { bytes, MAX_SCRIPT_BYTES } from "./limits.ts";
import { makePrepare, makeWatchState } from "./prepare.ts";
import type { SourceError } from "./source.ts";
import { inputJsonSchema } from "./tool.ts";
import { makeTransform } from "./transform.ts";
import { Upstream } from "./upstream.ts";
import { runWatch, type NotificationType } from "./watch.ts";

const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

const CLIENT_WATCH_TOOL = "watch_mcp_event";

interface ServerOptions {
  readonly catalog: Catalog.Interface;
  readonly upstream: Upstream.Interface;
  readonly decide: Option.Option<Decide>;
  readonly defaultExpiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
}

const streamParams = Schema.toStandardSchemaV1(
  Schema.Struct({
    name: Schema.String,
    arguments: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    cursor: Schema.optionalKey(Schema.Unknown),
  }),
);

// Stdout carries MCP messages, so handler logs must go to stderr.
const run = <A, E>(effect: Effect.Effect<A, E>, options?: Effect.RunOptions) =>
  Effect.runPromise(effect.pipe(Effect.provideService(Logger.LogToStderr, true)), options);

const invalidParams = (message: string) => new ProtocolError(INVALID_PARAMS, message);

const patternedParamHint = (params: JsonSchema.JsonSchema, name: string): string | undefined => {
  if (!isObject(params)) return undefined;
  const properties = params["properties"];
  if (!isObject(properties)) return undefined;
  const property = properties[name];
  if (!isObject(property) || typeof property["pattern"] !== "string") return undefined;
  const description = property["description"];
  return typeof description === "string" ? `${name}: ${description}` : undefined;
};

const paramsError = (
  event: RegisteredEvent,
  error: Schema.SchemaError,
  args: Schema.JsonObject,
): string => {
  const issues = SchemaIssue.makeFormatterStandardSchemaV1()(error.issue).issues;
  const paramName = (issue: (typeof issues)[number]): string | undefined => {
    const path = issue.path ?? [];
    return path.length === 1 && typeof path[0] === "string" ? path[0] : undefined;
  };
  // A union of required params also reports branches the caller did not choose,
  // so prefer a supplied param that failed over a missing one.
  const supplied = Object.keys(args);
  const failed = issues.filter((issue) => {
    const name = paramName(issue);
    return name !== undefined && supplied.includes(name);
  });
  const details = new Set(
    (failed.length === 0 ? issues : failed).map((issue) => {
      const name = paramName(issue);
      const hint =
        name === undefined || !supplied.includes(name)
          ? undefined
          : patternedParamHint(event.params, name);
      if (hint !== undefined) return `${hint} (${issue.message})`;
      const path = issue.path ?? [];
      const where = path.length === 0 ? "" : `\n  at ${JSON.stringify(path)}`;
      return `${issue.message}${where}`;
    }),
  );
  return `Invalid params for ${event.name}: ${[...details].join("\n")}`;
};

const text = (value: unknown) => ({
  content: [
    { type: "text" as const, text: typeof value === "string" ? value : JSON.stringify(value) },
  ],
});

interface Watch {
  readonly view: {
    readonly eventName: string;
    readonly startedAt: string;
    readonly expiresAt: string;
    readonly arguments: Record<string, unknown>;
  };
  readonly ended: Deferred.Deferred<string>;
  readonly finished: Deferred.Deferred<void>;
}

const buildServer = (options: ServerOptions): McpServer => {
  const transformEvent = makeTransform();
  const watches = new Map<string, Watch>();
  const toolPaths = options.catalog.sources.flatMap((source) =>
    Object.keys(source.tools).map((name) => `tools.${source.name}.${name}`),
  );
  const server = new McpServer(
    { name: "events-mcp", version: "0.0.0" },
    {
      capabilities: { extensions: { [EVENTS_EXTENSION]: {} } },
      instructions: [
        `Arm watches with ${CLIENT_WATCH_TOOL}; pass only server to list events. A watch runs filter → prepare → transform, and the transform string is delivered to the session.`,
        ...options.catalog.sources.flatMap((source) =>
          source.instructions === undefined ? [] : [source.instructions],
        ),
        'On "<step> failed", fix that step and arm again.',
      ].join("\n\n"),
    },
  );

  for (const source of options.catalog.sources)
    for (const [name, tool] of Object.entries(source.tools))
      server.registerTool(
        `${source.name}_${name}`,
        {
          description: tool.description,
          inputSchema: fromJsonSchema({
            ...inputJsonSchema(tool),
            additionalProperties: false,
          }),
        },
        (input) =>
          run(
            Effect.match(tool.run(input), {
              onFailure: (error) => ({ ...text(error.message), isError: true }),
              onSuccess: text,
            }),
          ),
      );

  server.registerTool(
    "list_watches",
    {
      description: "Active watches and their arguments.",
    },
    () =>
      text({
        watches: [...watches].map(([watchId, watch]) => ({ watchId, ...watch.view })),
      }),
  );
  server.registerTool(
    "stop_watch",
    {
      description: "Stop a watch and wait for it.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: {
            type: "string",
            minLength: 1,
            description: "From list_watches.",
          },
        },
        required: ["watchId"],
        additionalProperties: false,
      }),
    },
    ({ watchId }) =>
      run(
        Effect.gen(function* () {
          const watch = watches.get(watchId);
          if (watch) {
            yield* Deferred.succeed(watch.ended, "Stopped");
            yield* Deferred.await(watch.finished);
          }
          return text({ watchId, stopped: watch !== undefined });
        }),
      ),
  );

  server.server.setRequestHandler(
    "events/list",
    { params: Schema.toStandardSchemaV1(Schema.Struct({})) },
    () => ({
      events: options.catalog.events.map((event) => ({
        name: event.name,
        description: event.description || event.name,
        delivery: ["push"],
        inputSchema: inputSchemaDocument({
          filter: event.filter,
          params: event.params,
          toolPaths,
          decide: Option.isSome(options.decide),
          defaultExpiresInSeconds: options.defaultExpiresInSeconds,
        }),
        payloadSchema: { anyOf: [event.payload, { type: "string" }] },
      })),
    }),
  );

  server.server.setRequestHandler("events/stream", { params: streamParams }, (params, ctx) => {
    const notify = (type: NotificationType, body: Record<string, unknown> = {}) =>
      Effect.promise(() =>
        ctx.mcpReq.notify({
          method: `notifications/events/${type}`,
          params: {
            ...body,
            cursor: null,
            _meta: { [SUBSCRIPTION_ID_META_KEY]: ctx.mcpReq.id },
          },
        }),
      );
    return run(
      Effect.gen(function* () {
        if (params.cursor !== undefined && params.cursor !== null)
          return yield* Effect.fail(
            invalidParams("Events are live-only; omit cursor or set it to null"),
          );
        const event = options.catalog.get(params.name);
        if (!event)
          return yield* Effect.fail(
            invalidParams(`Unknown event: ${params.name}; choose a listed event`),
          );
        const decoded = yield* decodeArguments({
          filter: event.filter,
          value: params.arguments ?? {},
        }).pipe(Effect.mapError((error) => invalidParams(error.message)));
        for (const [name, source] of [
          ["Transform", decoded.transform],
          ["Prepare", decoded.prepare],
        ] as const) {
          if (bytes(source) > MAX_SCRIPT_BYTES)
            return yield* Effect.fail(
              invalidParams(`${name} source exceeds ${MAX_SCRIPT_BYTES} bytes; shorten it`),
            );
        }
        const eventParams = yield* Schema.decodeUnknownEffect(event.paramsSchema)(decoded.params, {
          onExcessProperty: "error",
        }).pipe(
          Effect.mapError((error) => invalidParams(paramsError(event, error, decoded.params))),
        );
        const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
        const ended = yield* Deferred.make<string>();
        const finished = yield* Deferred.make<void>();
        const startedAt = yield* Clock.currentTimeMillis;
        const watch: Watch = {
          view: {
            eventName: params.name,
            startedAt: new Date(startedAt).toISOString(),
            expiresAt: new Date(startedAt + expiresInSeconds * 1000).toISOString(),
            arguments: params.arguments ?? {},
          },
          ended,
          finished,
        };
        yield* Effect.acquireRelease(
          Effect.gen(function* () {
            if (watches.has(decoded.watchId))
              return yield* Effect.fail(
                invalidParams("watchId is already active; choose another ID or stop the watch"),
              );
            watches.set(decoded.watchId, watch);
          }),
          () =>
            Effect.gen(function* () {
              watches.delete(decoded.watchId);
              yield* Deferred.succeed(finished, undefined);
            }),
        );
        const state = yield* makeWatchState;
        const prepare = makePrepare({
          sources: options.catalog.sources,
          state,
          decide: options.decide,
        });
        yield* runWatch({
          watchId: decoded.watchId,
          eventType: event,
          params: eventParams,
          filter: decoded.filter,
          expiresInSeconds,
          eventTimeoutSeconds: options.eventTimeoutSeconds,
          ended,
          prepare: (occurrence) => prepare(decoded.prepare, occurrence),
          transform: (input) => transformEvent(decoded.transform, input),
          notify,
        }).pipe(
          Effect.provideService(Upstream.Service, options.upstream),
          Effect.mapError((error: SourceError) => {
            const code = error._tag === "Source.Rejected" ? -32024 : -32603;
            return new ProtocolError(code, error.message);
          }),
        );
        return {};
      }).pipe(Effect.scoped),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};

export interface Interface {
  readonly done: Effect.Effect<void>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Server") {}

export const make: Effect.Effect<
  McpServer,
  Config.ConfigError,
  Catalog.Service | Upstream.Service | Decisions.Service
> = Effect.gen(function* () {
  const catalog = yield* Catalog.Service;
  const upstream = yield* Upstream.Service;
  const decisions = yield* Decisions.Service;
  const defaultExpiresInSeconds = yield* watchDefaultSeconds;
  const eventTimeoutSeconds = yield* stepTimeoutSeconds;
  return buildServer({
    catalog,
    upstream,
    decide: decisions.decide,
    defaultExpiresInSeconds,
    eventTimeoutSeconds,
  });
});

// Stdout carries MCP messages, so transport errors must go to stderr.
const logError = (error: unknown): void => {
  Effect.runSync(Effect.logError(error).pipe(Effect.provideService(Logger.LogToStderr, true)));
};

export const layer: Layer.Layer<
  Service,
  Config.ConfigError,
  Catalog.Service | Upstream.Service | Decisions.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const server = yield* make;
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        serveStdio(() => server, {
          legacy: "reject",
          onerror: logError,
        }),
      ),
      (handle) => Effect.promise(() => handle.close()),
    );
    const done = Effect.callback<void>((resume) => {
      if (process.stdin.readableEnded) return resume(Effect.void);
      const onEnd = () => resume(Effect.void);
      process.stdin.once("end", onEnd);
      return Effect.sync(() => process.stdin.off("end", onEnd));
    });
    return Service.of({ done });
  }),
);

export * as Server from "./server.ts";
