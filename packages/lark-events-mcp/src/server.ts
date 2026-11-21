import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { Clock, Deferred, Effect, Logger, Schema } from "effect";
import type { Catalog } from "./catalog.ts";
import { decodeArguments, inputSchemaDocument } from "./contract.ts";
import { bytes, MAX_SCRIPT_BYTES } from "./limits.ts";
import type { DecisionsRequester } from "./prepare.ts";
import { makePrepare, makeWatchState } from "./prepare.ts";
import { SourceError } from "./source.ts";
import { inputJsonSchema } from "./tool.ts";
import { makeTransform } from "./transform.ts";
import type { Upstream } from "./upstream.ts";
import { runWatch, type NotificationType } from "./watch.ts";

const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

const CLIENT_WATCH_TOOL = "watch_mcp_event";

interface ServerOptions {
  readonly catalog: Catalog;
  readonly upstream: Upstream;
  readonly defaultExpiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
  readonly decide?: DecisionsRequester | undefined;
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

export const makeServer = (options: ServerOptions): McpServer => {
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
              onFailure: (error) => ({ ...text(error), isError: true }),
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
          decide: options.decide !== undefined,
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
        }).pipe(Effect.mapError(invalidParams));
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
          Effect.mapError((issue) =>
            invalidParams(`Invalid params for ${event.name}: ${issue.message}`),
          ),
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
          upstream: options.upstream,
          prepare: (occurrence) => prepare(decoded.prepare, occurrence),
          transform: (input) => transformEvent(decoded.transform, input),
          notify,
        }).pipe(
          Effect.mapError((error) => {
            const { code, message } = SourceError.$match(error, {
              Rejected: ({ message }) => ({ code: -32024, message }),
              Unavailable: ({ message }) => ({ code: -32603, message }),
            });
            return new ProtocolError(code, message);
          }),
        );
        return {};
      }).pipe(Effect.scoped),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};
