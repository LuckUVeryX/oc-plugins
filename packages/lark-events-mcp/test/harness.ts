import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport, SUBSCRIPTION_ID_META_KEY } from "@modelcontextprotocol/server";
import { Effect, PubSub, Schema } from "effect";
import { catalogLayer, LarkCatalog } from "../src/catalog.ts";
import type { SourceMessage } from "../src/producers.ts";
import { makeServer } from "../src/server.ts";

type Options = Parameters<typeof makeServer>[0];
type Event = Extract<SourceMessage, { kind: "event" }>;

export async function connect(
  options: {
    maxSeconds?: number;
    decide?: Options["decide"];
    eventTimeoutSeconds?: number;
    larkRequest?: Options["larkRequest"];
    transport?: (transport: ReturnType<typeof InMemoryTransport.createLinkedPair>[1]) => void;
  } = {},
) {
  const messages = await Effect.runPromise(PubSub.unbounded<SourceMessage>());
  const catalog = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* LarkCatalog;
    }).pipe(Effect.provide(catalogLayer)),
  );
  const server = makeServer({
    catalog,
    maxSeconds: options.maxSeconds ?? 60,
    decide: options.decide,
    eventTimeoutSeconds: options.eventTimeoutSeconds ?? 30,
    larkRequest: options.larkRequest ?? (async () => ({ code: 0, msg: "ok", data: null })),
    producers: {
      larkRequest: options.larkRequest ?? (async () => ({ code: 0, msg: "ok", data: null })),
      acquire: () =>
        Effect.succeed({
          messages,
          ready: Effect.void,
          terminal: Effect.succeed(undefined),
        }),
    },
  });
  const client = new Client({ name: "test", version: "1" });
  const notifications: Array<{
    type: string;
    id: string | number;
    eventId?: string;
    data?: unknown;
    timestamp?: string;
    origin?: string;
    reason?: string;
  }> = [];
  const params = Schema.toStandardSchemaV1(
    Schema.Struct({
      cursor: Schema.Null,
      _meta: Schema.Struct({
        [SUBSCRIPTION_ID_META_KEY]: Schema.Union([Schema.String, Schema.Number]),
        "lark-events-mcp/timestampOrigin": Schema.optionalKey(Schema.String),
      }),
      eventId: Schema.optionalKey(Schema.String),
      timestamp: Schema.optionalKey(Schema.String),
      data: Schema.optionalKey(Schema.Unknown),
      error: Schema.optionalKey(Schema.Struct({ code: Schema.Number, message: Schema.String })),
    }),
  );
  for (const type of ["active", "heartbeat", "event", "error", "terminated"]) {
    client.setNotificationHandler(`notifications/events/${type}`, { params }, (value) => {
      notifications.push({
        type,
        id: value._meta[SUBSCRIPTION_ID_META_KEY],
        ...(value.eventId !== undefined && { eventId: value.eventId }),
        ...(value.timestamp !== undefined && { timestamp: value.timestamp }),
        ...(value._meta["lark-events-mcp/timestampOrigin"] !== undefined && {
          origin: value._meta["lark-events-mcp/timestampOrigin"],
        }),
        ...(value.data !== undefined && { data: value.data }),
        ...(value.error && { reason: value.error.message }),
      });
    });
  }
  const [a, b] = InMemoryTransport.createLinkedPair();
  options.transport?.(b);
  await server.connect(b);
  await client.connect(a);
  const controllers: AbortController[] = [];
  const requests: Promise<unknown>[] = [];
  const result = Schema.toStandardSchemaV1(Schema.Unknown);
  return {
    client,
    notifications,
    stream(args: Record<string, unknown>, name = "im.message.receive_v1") {
      const controller = new AbortController();
      controllers.push(controller);
      const done = client
        .request({ method: "events/stream", params: { name, arguments: args } }, result, {
          signal: controller.signal,
          timeout: 120_000,
        })
        .catch((error: unknown) => error);
      requests.push(done);
      return { done, abort: () => controller.abort() };
    },
    publish: (event: Partial<Omit<Event, "kind">> & Pick<Event, "eventId" | "payload">) =>
      Effect.runPromise(
        PubSub.publish(messages, {
          kind: "event",
          timestamp: "2026-01-01T00:00:00.000Z",
          timestampOrigin: "upstream",
          ...event,
        }),
      ),
    async call(name: string, args: Record<string, unknown> = {}) {
      const response = await client.request(
        { method: "tools/call", params: { name, arguments: args } },
        Schema.toStandardSchemaV1(
          Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
        ),
      );
      return JSON.parse(response.content[0]!.text);
    },
    async close() {
      for (const controller of controllers) controller.abort();
      await Promise.all(requests);
      await client.close();
      await server.close();
      await Effect.runPromise(PubSub.shutdown(messages));
    },
  };
}
