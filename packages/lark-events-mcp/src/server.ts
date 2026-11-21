import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { Cause, Clock, Deferred, Effect, Logger, PubSub, Schedule, Schema } from "effect";
import type { EventDefinition, LarkCatalog } from "./catalog.ts";
import { decodeArguments, inputSchemaDocument, matchesFilter } from "./contract.ts";
import type { EventProducers, SourceMessage } from "./producers.ts";
import { bytes, makeTransform } from "./transform.ts";
import {
  getDescription,
  getLark,
  GetInput,
  makePrepare,
  type DecisionsRequester,
  type LarkRequester,
} from "./prepare.ts";

export const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

// OpenCode cancels event subscriptions after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

interface ServerOptions {
  readonly catalog: Effect.Success<typeof LarkCatalog>;
  readonly producers: Effect.Success<typeof EventProducers>;
  readonly maxSeconds: number;
  readonly decide?: DecisionsRequester | undefined;
  readonly eventTimeoutSeconds: number;
  readonly larkRequest: LarkRequester;
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

const describe = (definition: EventDefinition): string => definition.description || definition.name;

interface Watch {
  readonly ended: Deferred.Deferred<string>;
  readonly finished: Deferred.Deferred<void>;
  readonly eventName: string;
  readonly startedAt: string;
  readonly expiresAt: string;
  readonly arguments: Record<string, unknown>;
}

const watches = new Map<string, Watch>();

type EventMessage = Extract<SourceMessage, { kind: "event" }>;

export const makeServer = (options: ServerOptions): McpServer => {
  const transformEvent = makeTransform();
  const prepareEvent = makePrepare(options.larkRequest, options.decide);
  const server = new McpServer(
    { name: "lark-events-mcp", version: "0.0.0" },
    {
      capabilities: { extensions: { [EVENTS_EXTENSION]: {} } },
      instructions:
        'Lark events: arm a watch with watch_mcp_event. Before writing prepare, call get on each endpoint it reads to see the real response. In prepare, gate each event with tools.events.decide when it is available: one decide call is much cheaper than waking the session. When a watch stops with "<step> failed", fix that step and arm a new watch.',
    },
  );

  server.registerTool(
    "get",
    {
      description: getDescription,
      inputSchema: fromJsonSchema<typeof GetInput.Type>({
        ...Schema.toJsonSchemaDocument(GetInput).schema,
        additionalProperties: false,
      }),
    },
    (input) =>
      run(
        getLark(options.larkRequest, input).pipe(
          Effect.map((data) => ({
            content: [{ type: "text" as const, text: JSON.stringify(data) }],
          })),
          Effect.catch((error) =>
            Effect.succeed({
              content: [{ type: "text" as const, text: String(error) }],
              isError: true,
            }),
          ),
        ),
      ),
  );

  server.registerTool(
    "list_watches",
    {
      description:
        "List active watches with their event, start and expiry times, and the arguments they were armed with.",
    },
    () => ({
      content: [
        {
          type: "text",
          text: JSON.stringify({
            watches: [...watches].map(([watchId, watch]) => ({
              watchId,
              eventName: watch.eventName,
              startedAt: watch.startedAt,
              expiresAt: watch.expiresAt,
              arguments: watch.arguments,
            })),
          }),
        },
      ],
    }),
  );
  server.registerTool(
    "stop_watch",
    {
      description: "Stop a watch by ID and wait for it to stop.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: {
            type: "string",
            minLength: 1,
            description: "Use the watch ID or find it with list_watches.",
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
    () => ({
      events: options.catalog.events.map((definition) => ({
        name: definition.name,
        description: describe(definition),
        delivery: ["push"],
        inputSchema: inputSchemaDocument(definition.payloadSchema),
        payloadSchema: { anyOf: [definition.payloadSchema, { type: "string" }] },
      })),
    }),
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
    return run(
      Effect.gen(function* () {
        if (params.cursor !== undefined && params.cursor !== null)
          return yield* Effect.fail(
            invalidParams("Events are live-only; omit cursor or set it to null"),
          );
        const definition = options.catalog.get(params.name);
        if (!definition)
          return yield* Effect.fail(
            invalidParams(`Unknown event: ${params.name}; choose a listed event`),
          );
        const { filter, watchId, prepare, transform } = yield* decodeArguments(
          definition.payloadSchema,
          params.arguments ?? {},
        ).pipe(Effect.mapError(invalidParams));
        for (const [name, source] of [
          ["Transform", transform],
          ["Prepare", prepare],
        ] as const) {
          if (bytes(source) > 65536)
            return yield* Effect.fail(
              invalidParams(`${name} source exceeds 65536 bytes; shorten it`),
            );
        }
        const ended = yield* Deferred.make<string>();
        const finished = yield* Deferred.make<void>();
        const startedAt = yield* Clock.currentTimeMillis;
        const watch: Watch = {
          ended,
          finished,
          eventName: params.name,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + options.maxSeconds * 1000).toISOString(),
          arguments: params.arguments ?? {},
        };
        yield* Effect.acquireRelease(
          Effect.suspend(() => {
            if (watches.has(watchId))
              return Effect.fail(
                invalidParams("watchId is already active; choose another ID or stop the watch"),
              );
            watches.set(watchId, watch);
            return Effect.void;
          }),
          () =>
            Effect.gen(function* () {
              watches.delete(watchId);
              yield* Deferred.succeed(finished, undefined);
            }),
        );
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
        const deliverable = (message: EventMessage) => {
          let step = "prepare";
          const event = {
            eventId: message.eventId,
            timestamp: message.timestamp,
            timestampOrigin: message.timestampOrigin,
            data: message.payload,
          };
          const pipeline = Effect.gen(function* () {
            const prepared = yield* prepareEvent(prepare, event);
            if (prepared === null) return null;
            step = "transform";
            return yield* transformEvent(transform, { event, prepared });
          });
          return pipeline.pipe(
            Effect.timeout(`${options.eventTimeoutSeconds} seconds`),
            Effect.catchCause((cause) => {
              if (Cause.hasInterruptsOnly(cause)) return Effect.failCause(cause);
              return Effect.logWarning("Event step failed").pipe(
                Effect.annotateLogs({ watchId, eventId: message.eventId, step }),
                Effect.andThen(
                  Effect.fail(
                    `${step} failed on ${message.eventId}: ${String(Cause.squash(cause))}`,
                  ),
                ),
              );
            }),
          );
        };
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
                const outcome = yield* deliverable(message).pipe(Effect.result);
                if (outcome._tag === "Failure") return outcome.failure;
                const data = outcome.success;
                if (data === null) break;
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
