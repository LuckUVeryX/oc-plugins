import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { Clock, Deferred, Effect, PubSub, Schedule, Schema } from "effect";
import type { EventDefinition, LarkCatalog } from "./catalog.ts";
import { decodeArguments, inputSchemaDocument, matchesFilter } from "./contract.ts";
import type { EventProducers } from "./producers.ts";

export const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

// OpenCode cancels event subscriptions after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";
const BATCH_LIMIT = 16_384;

interface ServerOptions {
  readonly catalog: Effect.Success<typeof LarkCatalog>;
  readonly producers: Effect.Success<typeof EventProducers>;
}

const streamParams = Schema.toStandardSchemaV1(
  Schema.Struct({
    name: Schema.String,
    arguments: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    cursor: Schema.optionalKey(Schema.Unknown),
  }),
);

const invalidParams = (message: string) => new ProtocolError(INVALID_PARAMS, message);

const describe = (definition: EventDefinition): string =>
  `Lark MCP push subscription. Use watch_mcp_event on lark-events. ${definition.description || definition.name}`;

interface Watch {
  readonly ended: Deferred.Deferred<string>;
  readonly finished: Deferred.Deferred<void>;
  readonly eventName: string;
  readonly startedAt: string;
  readonly expiresAt: string;
}

const watches = new Map<string, Watch>();

export const makeServer = (options: ServerOptions): McpServer => {
  const server = new McpServer(
    { name: "lark-events-mcp", version: "0.0.0" },
    { capabilities: { extensions: { [EVENTS_EXTENSION]: {} } } },
  );

  server.registerTool("watch_list", { description: "List active Lark MCP watches." }, () => ({
    content: [
      {
        type: "text",
        text: JSON.stringify({
          watches: [...watches].map(([watchId, { eventName, startedAt, expiresAt }]) => ({
            watchId,
            eventName,
            startedAt,
            expiresAt,
          })),
        }),
      },
    ],
  }));
  server.registerTool(
    "watch_stop",
    {
      description: "Stop an active Lark MCP watch and wait for cleanup.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: { type: "string", minLength: 1, description: "ID of the active watch." },
        },
        required: ["watchId"],
        additionalProperties: false,
      }),
    },
    ({ watchId }) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const watch = watches.get(watchId);
          if (watch) {
            yield* Deferred.succeed(watch.ended, "Stopped");
            yield* Deferred.await(watch.finished);
          }
          return {
            content: [
              {
                type: "text" as const,
                text: JSON.stringify({ watchId, stopped: watch !== undefined }),
              },
            ],
          };
        }),
      ),
  );

  server.server.setRequestHandler(
    "events/list",
    { params: Schema.toStandardSchemaV1(Schema.Struct({})) },
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const definitions = yield* options.catalog.list();
          return {
            events: definitions.map((definition) => ({
              name: definition.name,
              description: describe(definition),
              delivery: ["push"],
              inputSchema: inputSchemaDocument(definition.payloadSchema),
              payloadSchema: {
                type: "object",
                required: ["events"],
                properties: {
                  events: {
                    type: "array",
                    minItems: 1,
                    items: {
                      type: "object",
                      required: ["eventId", "timestamp", "timestampOrigin", "data"],
                      properties: {
                        eventId: { type: "string" },
                        timestamp: { type: "string" },
                        timestampOrigin: { enum: ["upstream", "local"] },
                        data: definition.payloadSchema,
                      },
                    },
                  },
                },
              },
            })),
          };
        }),
      ),
  );

  server.server.setRequestHandler("events/stream", { params: streamParams }, (params, ctx) => {
    const notify = (
      type: string,
      body: Record<string, unknown> = {},
      meta: Record<string, unknown> = {},
    ) =>
      Effect.tryPromise(() =>
        ctx.mcpReq.notify({
          method: `notifications/events/${type}`,
          params: {
            ...body,
            cursor: null,
            _meta: { ...meta, [SUBSCRIPTION_ID_META_KEY]: ctx.mcpReq.id },
          },
        }),
      );
    return Effect.runPromise(
      Effect.gen(function* () {
        if (params.cursor !== undefined && params.cursor !== null)
          return yield* Effect.fail(
            invalidParams("lark events are live-only; cursor must be null or omitted"),
          );
        const definition = yield* options.catalog
          .schema(params.name)
          .pipe(Effect.mapError((error) => invalidParams(error.message)));
        const decoded = yield* decodeArguments(definition.payloadSchema, params.arguments ?? {});
        if (decoded._tag === "error") return yield* Effect.fail(invalidParams(decoded.message));
        const { filter, watchId, durationSeconds, waitSeconds, maxWaitSeconds } = decoded.value;
        const ended = yield* Deferred.make<string>();
        const finished = yield* Deferred.make<void>();
        const startedAt = yield* Clock.currentTimeMillis;
        const watch: Watch = {
          ended,
          finished,
          eventName: params.name,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + durationSeconds * 1000).toISOString(),
        };
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            if (watches.get(watchId) === watch) watches.delete(watchId);
            yield* Deferred.succeed(finished, undefined);
          }),
        );
        const registered = yield* Effect.sync(() => {
          if (watches.has(watchId)) return false;
          watches.set(watchId, watch);
          return true;
        });
        if (!registered) return yield* Effect.fail(invalidParams("watchId is already active"));
        yield* Effect.sleep(durationSeconds * 1000).pipe(
          Effect.andThen(Deferred.succeed(ended, "Expired")),
          Effect.forkScoped,
        );
        const producer = yield* options.producers
          .acquire(params.name)
          .pipe(Effect.mapError((error) => new ProtocolError(-32603, error.message)));
        const subscription = yield* PubSub.subscribe(producer.messages);
        const ready = yield* Effect.raceFirst(
          producer.ready.pipe(
            Effect.as(true),
            Effect.mapError((error) => new ProtocolError(-32603, error.message)),
          ),
          Deferred.await(ended).pipe(Effect.as(false)),
        );
        if (!ready) {
          yield* notify("terminated", {
            error: { code: -32000, message: yield* Deferred.await(ended) },
          });
          return {};
        }
        type Item = {
          eventId: string;
          timestamp: string;
          timestampOrigin: "upstream" | "local";
          data: unknown;
        };
        type Batch = { events: Item[]; size: number; firstAt: number; lastAt: number };
        let batch: Batch | undefined;
        const flush = () =>
          Effect.gen(function* () {
            if (!batch) return;
            const events = batch.events;
            batch = undefined;
            const now = yield* Clock.currentTimeMillis;
            yield* notify(
              "event",
              {
                eventId: `local-${crypto.randomUUID()}`,
                name: params.name,
                timestamp: new Date(now).toISOString(),
                data: { events },
              },
              { "lark-events-mcp/timestampOrigin": "local" },
            );
          });
        const worker = Effect.gen(function* () {
          yield* notify("active");
          const recorded = yield* producer.terminal;
          if (recorded !== undefined) return recorded;
          let connected = true;
          while (true) {
            const now = yield* Clock.currentTimeMillis;
            const deadline =
              batch &&
              Math.min(batch.lastAt + waitSeconds * 1000, batch.firstAt + maxWaitSeconds * 1000);
            if (deadline !== undefined && deadline <= now) {
              yield* flush();
              continue;
            }
            const next =
              deadline === undefined
                ? yield* PubSub.take(subscription).pipe(
                    Effect.map((message) => ({ kind: "message" as const, message })),
                  )
                : yield* Effect.raceFirst(
                    PubSub.take(subscription).pipe(
                      Effect.map((message) => ({ kind: "message" as const, message })),
                    ),
                    Effect.sleep(deadline - now).pipe(Effect.as({ kind: "flush" as const })),
                  );
            if (next.kind === "flush") {
              yield* flush();
              continue;
            }
            const message = next.message;
            switch (message.kind) {
              case "event": {
                if (!matchesFilter(filter, message.payload)) break;
                const item: Item = {
                  eventId: message.eventId,
                  timestamp: message.timestamp,
                  timestampOrigin: message.timestampOrigin,
                  data: message.payload,
                };
                const itemSize = JSON.stringify(item).length;
                if (batch && batch.size + 1 + itemSize > BATCH_LIMIT) yield* flush();
                const at = yield* Clock.currentTimeMillis;
                if (batch) {
                  batch.events.push(item);
                  batch.size += 1 + itemSize;
                  batch.lastAt = at;
                } else {
                  batch = {
                    events: [item],
                    size: '{"events":[]}'.length + itemSize,
                    firstAt: at,
                    lastAt: at,
                  };
                }
                if (waitSeconds === 0 || batch.size > BATCH_LIMIT) yield* flush();
                break;
              }
              case "reconnecting":
                connected = false;
                yield* notify("error", {
                  error: {
                    code: -32000,
                    message: "Lark connection reconnecting; events may be missed",
                  },
                });
                break;
              case "reconnected":
                if (connected) break;
                connected = true;
                yield* notify("active");
                break;
              case "terminated":
                return message.message;
            }
          }
        });
        const heartbeat = notify("heartbeat").pipe(
          Effect.repeat(Schedule.spaced(HEARTBEAT_INTERVAL)),
          Effect.delay(HEARTBEAT_INTERVAL),
        );
        const reason = yield* Effect.raceFirst(
          Deferred.await(ended),
          Effect.raceFirst(worker, heartbeat),
        );
        if (reason === "Expired") yield* flush();
        yield* notify("terminated", { error: { code: -32000, message: reason } });
        return {};
      }).pipe(Effect.scoped),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};
