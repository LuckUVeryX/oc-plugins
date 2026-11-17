import {
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { Effect, Fiber, PubSub, Schedule, Schema, Stream } from "effect";
import type { EventDefinition, LarkCatalog } from "./catalog.ts";
import { decodeArguments, inputSchemaDocument, matchesFilter } from "./contract.ts";
import type { EventProducers } from "./producers.ts";

export const EVENTS_EXTENSION = "io.modelcontextprotocol/events";

// OpenCode cancels event subscriptions after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

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

const describe = (definition: EventDefinition): string => definition.description || definition.name;

export const makeServer = (options: ServerOptions): McpServer => {
  const server = new McpServer(
    { name: "lark-events-mcp", version: "0.0.0" },
    { capabilities: { extensions: { [EVENTS_EXTENSION]: {} } } },
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
              payloadSchema: definition.payloadSchema,
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
        const decoded = decodeArguments(definition.payloadSchema, params.arguments ?? {});
        if (decoded._tag === "error") return yield* Effect.fail(invalidParams(decoded.message));
        const filter = decoded.value;
        const producer = yield* options.producers
          .acquire(params.name)
          .pipe(Effect.mapError((error) => new ProtocolError(-32603, error.message)));
        const subscription = yield* PubSub.subscribe(producer.messages);
        yield* producer.ready.pipe(
          Effect.mapError((error) => new ProtocolError(-32603, error.message)),
        );
        yield* notify("active");
        const terminatedWith = (message: string) =>
          notify("terminated", { error: { code: -32000, message } });
        const recorded = yield* producer.terminal;
        if (recorded !== undefined) {
          yield* terminatedWith(recorded);
          return {};
        }
        const heartbeat = yield* Stream.fromSchedule(Schedule.spaced(HEARTBEAT_INTERVAL)).pipe(
          Stream.runForEach(() => notify("heartbeat")),
          Effect.forkScoped,
        );
        let terminationReason: string | undefined;
        let connected = true;
        yield* Stream.fromSubscription(subscription).pipe(
          Stream.takeUntil((message) => message.kind === "terminated"),
          Stream.runForEach((message) => {
            switch (message.kind) {
              case "event":
                return matchesFilter(filter, message.payload)
                  ? notify(
                      "event",
                      {
                        eventId: message.eventId,
                        name: params.name,
                        timestamp: message.timestamp,
                        data: message.payload,
                      },
                      { "lark-events-mcp/timestampOrigin": message.timestampOrigin },
                    )
                  : Effect.void;
              case "reconnecting":
                connected = false;
                return notify("error", {
                  error: {
                    code: -32000,
                    message: "Lark connection reconnecting; events may be missed",
                  },
                });
              case "reconnected":
                if (connected) return Effect.void;
                connected = true;
                return notify("active");
              case "terminated":
                terminationReason = message.message;
                return Effect.void;
            }
          }),
        );
        yield* Fiber.interrupt(heartbeat);
        yield* terminatedWith(terminationReason ?? "event source closed");
        return {};
      }).pipe(Effect.scoped),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};
