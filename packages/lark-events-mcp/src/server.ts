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
import { bytes, makeTransform } from "./transform.ts";
import type { makeDecisions } from "./decisions.ts";

export const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

// OpenCode cancels event subscriptions after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

interface ServerOptions {
  readonly catalog: Effect.Success<typeof LarkCatalog>;
  readonly producers: Effect.Success<typeof EventProducers>;
  readonly maxSeconds: number;
  readonly decide?: ReturnType<typeof makeDecisions> | undefined;
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
  `Watch this Lark event with watch_mcp_event on lark-events. ${definition.description || definition.name}`;

interface Watch {
  readonly ended: Deferred.Deferred<string>;
  readonly finished: Deferred.Deferred<void>;
  readonly eventName: string;
  readonly startedAt: string;
  readonly expiresAt: string;
}

const watches = new Map<string, Watch>();

export const makeServer = (options: ServerOptions): McpServer => {
  const transformEvent = makeTransform();
  const server = new McpServer(
    { name: "lark-events-mcp", version: "0.0.0" },
    { capabilities: { extensions: { [EVENTS_EXTENSION]: {} } } },
  );

  server.registerTool(
    "list_watches",
    { description: "List active Lark event watches with their IDs and expiry times." },
    () => ({
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
    }),
  );
  server.registerTool(
    "stop_watch",
    {
      description:
        "Stop a Lark event watch by ID before it expires. Returns once the watch has stopped.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: {
            type: "string",
            minLength: 1,
            description: "Use the ID from watch registration or list_watches.",
          },
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
              payloadSchema: { anyOf: [definition.payloadSchema, { type: "string" }] },
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
        const { filter, watchId, transform, decisions } = decoded.value;
        if (decisions !== undefined && options.decide === undefined)
          return yield* Effect.fail(invalidParams("decisions model is not configured"));
        if (transform !== undefined && bytes(transform) > 65536)
          return yield* Effect.fail(invalidParams("transform source exceeds 65536 bytes"));
        const ended = yield* Deferred.make<string>();
        const finished = yield* Deferred.make<void>();
        const startedAt = yield* Clock.currentTimeMillis;
        const watch: Watch = {
          ended,
          finished,
          eventName: params.name,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + options.maxSeconds * 1000).toISOString(),
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
        const now = yield* Clock.currentTimeMillis;
        yield* Effect.sleep(Math.max(0, startedAt + options.maxSeconds * 1000 - now)).pipe(
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
        const worker = Effect.gen(function* () {
          yield* notify("active");
          const recorded = yield* producer.terminal;
          if (recorded !== undefined) return recorded;
          let connected = true;
          while (true) {
            const message = yield* PubSub.take(subscription);
            switch (message.kind) {
              case "event": {
                if (!matchesFilter(filter, message.payload)) break;
                let data: unknown = message.payload;
                if (transform !== undefined) {
                  const event = {
                    eventId: message.eventId,
                    timestamp: message.timestamp,
                    timestampOrigin: message.timestampOrigin,
                    data: message.payload,
                  };
                  let input: unknown = { event };
                  if (decisions !== undefined && options.decide !== undefined) {
                    const decision = yield* options
                      .decide(decisions, event)
                      .pipe(
                        Effect.catch((kind) =>
                          Effect.logWarning("Event decisions failed").pipe(
                            Effect.annotateLogs({ watchId, eventId: message.eventId, kind }),
                            Effect.as(null),
                          ),
                        ),
                      );
                    if (decision === null) break;
                    input = { event, result: decision };
                  }
                  const result = yield* transformEvent(transform, input).pipe(
                    Effect.catch((kind) =>
                      Effect.logWarning("Event transform failed").pipe(
                        Effect.annotateLogs({ watchId, eventId: message.eventId, kind }),
                        Effect.as(null),
                      ),
                    ),
                  );
                  if (result === null) break;
                  data = result;
                }
                yield* notify(
                  "event",
                  {
                    eventId: message.eventId,
                    name: params.name,
                    timestamp: message.timestamp,
                    data,
                  },
                  { "lark-events-mcp/timestampOrigin": message.timestampOrigin },
                );
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
        yield* notify("terminated", { error: { code: -32000, message: reason } });
        return {};
      }).pipe(Effect.scoped),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};
