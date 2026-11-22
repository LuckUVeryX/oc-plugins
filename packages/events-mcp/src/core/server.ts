import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { serveStdio, StdioServerTransport } from "@modelcontextprotocol/server/stdio";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Logger,
  Schema,
  SchemaIssue,
  Scope,
} from "effect";
import type { JsonSchema } from "effect";
import { isObject } from "effect/Predicate";
import {
  decodeArguments,
  expiresInSecondsProperty,
  filterProperty,
  inputSchemaDocument,
  Invalid,
  prepareProperty,
  transformProperty,
  watchIdProperty,
  WatchArguments,
} from "./arguments.ts";
import { Registry, type RegisteredEventKind } from "./registry.ts";
import { DecisionModel, type Decide } from "./decision-model.ts";
import { bytes, MAX_SCRIPT_BYTES } from "./limits.ts";
import { makePrepare, makeWatchState } from "./prepare.ts";
import type { IntegrationError } from "./integration.ts";
import { inputJsonSchema } from "./tool.ts";
import { makeTransform } from "./transform.ts";
import { Feeds } from "./feeds.ts";
import { runWatch, type NotificationType } from "./watch.ts";
import type { Harness, Send, Tools } from "./harness.ts";
import { get, type Name as HarnessName } from "../harness/index.ts";

const DEFAULT_WATCH_SECONDS = 3600;
const DEFAULT_STEP_TIMEOUT_SECONDS = 120;

interface ServerOptions {
  readonly harness: Harness;
  readonly scope: Scope.Scope;
  readonly registry: Registry.Interface;
  readonly feeds: Feeds.Interface;
  readonly decide: Decide;
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
  event: RegisteredEventKind,
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

const watchInputSchema = (options: {
  readonly watchParams: Tools["watchParams"];
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}) => {
  const { schema, definitions } = Schema.toJsonSchemaDocument(options.watchParams);
  const properties = isObject(schema["properties"]) ? schema["properties"] : {};
  const required = Array.isArray(schema["required"])
    ? schema["required"].filter((name) => typeof name === "string")
    : [];
  return {
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
    type: "object",
    properties: {
      event: {
        type: "string",
        description: "The event to watch, from list_events.",
      },
      watchId: watchIdProperty,
      params: {
        type: "object",
        description: "Selects the resource to watch. list_events describes this event's params.",
      },
      filter: filterProperty({ hint: "list_events lists the paths for each event." }),
      prepare: prepareProperty(options.toolPaths),
      transform: transformProperty,
      expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
      ...properties,
    },
    required: ["event", "watchId", "prepare", "transform", ...required],
    additionalProperties: false,
  };
};

const buildServer = (options: ServerOptions): McpServer => {
  const transformEvent = makeTransform();
  const watches = new Map<string, Watch>();
  const toolPaths = options.registry.integrations.flatMap((integration) =>
    Object.keys(integration.tools).map((name) => `tools.${integration.name}.${name}`),
  );
  const server = new McpServer(
    { name: "events-mcp", version: "0.0.0" },
    {
      capabilities: options.harness.capabilities,
      instructions: options.harness.instructions(options.registry.integrations),
    },
  );

  for (const integration of options.registry.integrations)
    for (const [name, tool] of Object.entries(integration.tools))
      server.registerTool(
        `${integration.name}_${name}`,
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
      description:
        "List open watches with their event, arguments, and expiry, so you can reuse the arguments or find a watchId. A watch that already ended is not listed.",
    },
    () =>
      text({
        watches: [...watches].map(([watchId, watch]) => ({ watchId, ...watch.view })),
      }),
  );
  server.registerTool(
    "stop_watch",
    {
      description:
        "End a watch early and wait until it has closed. Returns { watchId, stopped }. stopped is false when no open watch has that id.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: {
            type: "string",
            minLength: 1,
            description: "The watch to end, from list_watches or from when you armed it.",
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

  const catalogue = () =>
    options.registry.events.map((event) => ({
      name: event.name,
      description: event.description || event.name,
      delivery: ["push"],
      inputSchema: inputSchemaDocument({
        filter: event.filter,
        params: event.params,
        toolPaths,
        defaultExpiresInSeconds: options.defaultExpiresInSeconds,
      }),
      payloadSchema: { anyOf: [event.payload, { type: "string" }] },
    }));

  const resolveWatch = (name: string, value: unknown) =>
    Effect.gen(function* () {
      const event = options.registry.get(name);
      if (!event)
        return yield* Effect.fail(
          new Invalid({ message: `Unknown event: ${name}; choose a listed event` }),
        );
      const decoded = yield* decodeArguments({ filter: event.filter, value });
      for (const [step, code] of [
        ["Transform", decoded.transform],
        ["Prepare", decoded.prepare],
      ] as const) {
        if (bytes(code) > MAX_SCRIPT_BYTES)
          return yield* Effect.fail(
            new Invalid({
              message: `${step} source exceeds ${MAX_SCRIPT_BYTES} bytes; shorten it`,
            }),
          );
      }
      const eventParams = yield* Schema.decodeUnknownEffect(event.paramsSchema)(decoded.params, {
        onExcessProperty: "error",
      }).pipe(
        Effect.mapError(
          (error) => new Invalid({ message: paramsError(event, error, decoded.params) }),
        ),
      );
      return { event, decoded, eventParams };
    });

  const newWatch = (watch: {
    readonly eventName: string;
    readonly arguments: Record<string, unknown>;
    readonly expiresInSeconds: number;
  }) =>
    Effect.gen(function* () {
      const startedAt = yield* Clock.currentTimeMillis;
      const ended = yield* Deferred.make<string>();
      const finished = yield* Deferred.make<void>();
      return {
        view: {
          eventName: watch.eventName,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + watch.expiresInSeconds * 1000).toISOString(),
          arguments: watch.arguments,
        },
        ended,
        finished,
      } satisfies Watch;
    });

  if (options.harness._tag === "Tools") {
    const harness = options.harness;
    const send: Send = (notification) =>
      Effect.promise(() => server.server.notification(notification));
    const WatchInput = Schema.Struct({
      event: Schema.String,
      ...WatchArguments.fields,
      ...harness.watchParams.fields,
    });
    const watchTool = watchInputSchema({
      watchParams: harness.watchParams,
      toolPaths,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds,
    });
    server.registerTool(
      "list_events",
      {
        description:
          "List the events you can watch, with each event's parameters, payload, and filter paths. Read it before you arm a watch.",
      },
      () => text({ events: catalogue() }),
    );
    server.registerTool(
      "watch",
      {
        description:
          "Arm a watch and return once it is live. Each event that passes filter and prepare reaches this session as the transform result, until the watch expires or you stop it. Read the event's fields from list_events before you write prepare and transform.",
        inputSchema: fromJsonSchema<typeof WatchInput.Type>(watchTool),
      },
      (input) =>
        run(
          Effect.match(
            Effect.gen(function* () {
              const {
                event: name,
                watchId,
                params,
                filter,
                prepare: prepareSource,
                transform,
                expiresInSeconds: requestedExpiresInSeconds,
                ...session
              } = yield* Schema.decodeUnknownEffect(WatchInput)(input).pipe(
                Effect.mapError((error) => new Invalid({ message: error.message })),
              );
              const { event, decoded, eventParams } = yield* resolveWatch(name, {
                watchId,
                ...(params === undefined ? {} : { params }),
                ...(filter === undefined ? {} : { filter }),
                prepare: prepareSource,
                transform,
                ...(requestedExpiresInSeconds === undefined
                  ? {}
                  : { expiresInSeconds: requestedExpiresInSeconds }),
              });
              const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
              const { view, ended, finished } = yield* newWatch({
                eventName: event.name,
                arguments: input,
                expiresInSeconds,
              });
              const ready = yield* Deferred.make<string | undefined, IntegrationError>();
              const claimed = yield* Effect.sync(() => {
                if (watches.has(decoded.watchId)) return false;
                watches.set(decoded.watchId, { view, ended, finished });
                return true;
              });
              if (!claimed)
                return yield* Effect.fail(
                  new Invalid({
                    message: "watchId is already active; choose another ID or stop the watch",
                  }),
                );
              const state = yield* makeWatchState;
              const prepare = makePrepare({
                integrations: options.registry.integrations,
                state,
                decide: options.decide,
              });
              yield* runWatch({
                watchId: decoded.watchId,
                eventKind: event,
                params: eventParams,
                filter: decoded.filter,
                expiresInSeconds,
                eventTimeoutSeconds: options.eventTimeoutSeconds,
                ended,
                ready,
                prepare: (delivered) => prepare(decoded.prepare, delivered),
                transform: (transformInput) => transformEvent(decoded.transform, transformInput),
                notify: (type, body = {}) => {
                  if (type === "event")
                    return harness.wake(send, {
                      session,
                      watchId: decoded.watchId,
                      event: event.name,
                      text: typeof body.data === "string" ? body.data : JSON.stringify(body.data),
                    });
                  if (type !== "terminated") return Effect.void;
                  const error = body["error"];
                  const reason =
                    isObject(error) && typeof error["message"] === "string"
                      ? error["message"]
                      : "Watch ended";
                  if (reason === "Stopped") return Effect.void;
                  return harness.wake(send, {
                    session,
                    watchId: decoded.watchId,
                    event: event.name,
                    text: `Watch ${decoded.watchId} ended: ${reason}`,
                  });
                },
              }).pipe(
                Effect.provideService(Feeds.Service, options.feeds),
                Effect.onExit((exit) => {
                  const warn =
                    Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                      ? Effect.logWarning("Watch failed").pipe(
                          Effect.annotateLogs({
                            watchId: decoded.watchId,
                            error: String(Cause.squash(exit.cause)),
                          }),
                        )
                      : Effect.void;
                  return warn.pipe(Effect.andThen(Deferred.done(ready, exit)));
                }),
                Effect.ensuring(
                  Effect.gen(function* () {
                    watches.delete(decoded.watchId);
                    yield* Deferred.succeed(finished, undefined);
                  }),
                ),
                Effect.scoped,
                Effect.forkIn(options.scope),
              );
              const outcome = yield* Deferred.await(ready).pipe(
                Effect.mapError((error) => new Invalid({ message: error.message })),
              );
              return outcome === undefined
                ? { watchId: decoded.watchId, expiresAt: view.expiresAt }
                : { watchId: decoded.watchId, ended: outcome };
            }),
            {
              onFailure: (error) => ({ ...text(error.message), isError: true }),
              onSuccess: text,
            },
          ),
        ),
    );
  } else {
    server.server.setRequestHandler(
      "events/list",
      { params: Schema.toStandardSchemaV1(Schema.Struct({})) },
      () => ({ events: catalogue() }),
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
          const { event, decoded, eventParams } = yield* resolveWatch(
            params.name,
            params.arguments ?? {},
          ).pipe(Effect.mapError((error) => invalidParams(error.message)));
          const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
          const watch = yield* newWatch({
            eventName: params.name,
            arguments: params.arguments ?? {},
            expiresInSeconds,
          });
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
                yield* Deferred.succeed(watch.finished, undefined);
              }),
          );
          const state = yield* makeWatchState;
          const prepare = makePrepare({
            integrations: options.registry.integrations,
            state,
            decide: options.decide,
          });
          yield* runWatch({
            watchId: decoded.watchId,
            eventKind: event,
            params: eventParams,
            filter: decoded.filter,
            expiresInSeconds,
            eventTimeoutSeconds: options.eventTimeoutSeconds,
            ended: watch.ended,
            prepare: (event) => prepare(decoded.prepare, event),
            transform: (input) => transformEvent(decoded.transform, input),
            notify,
          }).pipe(
            Effect.provideService(Feeds.Service, options.feeds),
            Effect.mapError((error: IntegrationError) => {
              const code = error._tag === "Integration.Rejected" ? -32024 : -32603;
              return new ProtocolError(code, error.message);
            }),
          );
          return {};
        }).pipe(Effect.scoped),
        { signal: ctx.mcpReq.signal },
      );
    });
  }

  return server;
};

export interface Interface {
  readonly done: Effect.Effect<void>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Server") {}

export const make = (options: {
  readonly harness: HarnessName;
  /** Owns every watch this connection arms; closing it ends them. */
  readonly scope: Scope.Scope;
  readonly defaultExpiresInSeconds?: number;
  readonly eventTimeoutSeconds?: number;
}): Effect.Effect<McpServer, never, Registry.Service | Feeds.Service | DecisionModel.Service> =>
  Effect.gen(function* () {
    const registry = yield* Registry.Service;
    const feeds = yield* Feeds.Service;
    const decisionModel = yield* DecisionModel.Service;
    return buildServer({
      harness: get(options.harness),
      scope: options.scope,
      registry,
      feeds,
      decide: decisionModel.decide,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds ?? DEFAULT_WATCH_SECONDS,
      eventTimeoutSeconds: options.eventTimeoutSeconds ?? DEFAULT_STEP_TIMEOUT_SECONDS,
    });
  });

// Stdout carries MCP messages, so transport errors must go to stderr.
const logError = (error: unknown): void => {
  Effect.runSync(Effect.logError(error).pipe(Effect.provideService(Logger.LogToStderr, true)));
};

export const layer = (
  name: HarnessName,
): Layer.Layer<Service, never, Registry.Service | Feeds.Service | DecisionModel.Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const scope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));
      const closed = yield* Deferred.make<void>();
      const transport = new StdioServerTransport();
      const server = yield* make({ harness: name, scope });
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const handle = serveStdio(() => server, {
            legacy: "serve",
            transport,
            onerror: logError,
          });
          const onclose = transport.onclose;
          transport.onclose = () => {
            onclose?.();
            Deferred.doneUnsafe(closed, Effect.void);
          };
          return handle;
        }),
        (handle) => Effect.promise(() => handle.close()),
      );
      const done = Effect.gen(function* () {
        yield* Deferred.await(closed);
        yield* Scope.close(scope, Exit.void);
      }).pipe(Effect.asVoid);
      return Service.of({ done });
    }),
  );

export * as Server from "./server.ts";
