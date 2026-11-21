import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport, SUBSCRIPTION_ID_META_KEY } from "@modelcontextprotocol/server";
import { DateTime, Effect, Exit, PubSub, Schema, Scope, Stream, SubscriptionRef } from "effect";
import { type Catalog, makeCatalog } from "../src/core/catalog.ts";
import { makeGetTool, type LarkRequester } from "../src/lark/get.ts";
import { makeServer } from "../src/core/server.ts";
import {
  Health,
  SourceName,
  type EventType,
  type Occurrence,
  type Source,
  type SourceError,
} from "../src/core/source.ts";
import { makeUpstream, type Upstream } from "../src/core/upstream.ts";

type Options = Parameters<typeof makeServer>[0];

const lark = Schema.decodeUnknownSync(SourceName)("lark");
const other = Schema.decodeUnknownSync(SourceName)("other");
const larkMessage = "lark.im.message.receive_v1";
const larkParams: Schema.JsonObject = {
  type: "object",
  properties: { group: { type: "string" } },
  additionalProperties: false,
};
const eventParams: Schema.JsonObject = {
  type: "object",
  properties: {},
  additionalProperties: false,
};

interface Publish {
  readonly eventId?: string;
  readonly occurredAt?: string;
  readonly payload: Schema.JsonObject;
}

export async function connect(
  options: {
    defaultExpiresInSeconds?: number;
    decide?: Options["decide"];
    eventTimeoutSeconds?: number;
    request?: LarkRequester;
    otherSubscribeError?: SourceError | undefined;
    sources?: ReadonlyArray<Source>;
    transport?: (transport: ReturnType<typeof InMemoryTransport.createLinkedPair>[1]) => void;
  } = {},
) {
  const request: LarkRequester =
    options.request ?? (async () => ({ code: 0, msg: "ok", data: null }));
  const larkChannel = await Effect.runPromise(PubSub.unbounded<Occurrence>());
  const otherChannel = await Effect.runPromise(PubSub.unbounded<Occurrence>());
  const health = await Effect.runPromise(SubscriptionRef.make<Health>(Health.Live()));
  const sources: ReadonlyArray<Source> = options.sources ?? [
    {
      name: lark,
      tools: { get: makeGetTool(Effect.succeed(request)) },
      instructions: "Lark instructions.",
      events: [
        {
          name: "im.message.receive_v1",
          description: "A message was received.",
          params: larkParams,
          payload: {
            type: "object",
            properties: {
              message: { type: "object", properties: { chat_id: { type: "string" } } },
            },
          },
          subscribe: () =>
            Effect.gen(function* () {
              const subscription = yield* PubSub.subscribe(larkChannel);
              return {
                occurrences: Stream.fromSubscription(subscription),
                health: SubscriptionRef.changes(health),
              };
            }),
        },
      ] satisfies ReadonlyArray<EventType>,
    },
    {
      name: other,
      tools: {},
      instructions: "Other instructions.",
      events: [
        {
          name: "ping",
          description: "A ping.",
          params: eventParams,
          payload: { type: "object" },
          subscribe: () =>
            options.otherSubscribeError === undefined
              ? Effect.gen(function* () {
                  const subscription = yield* PubSub.subscribe(otherChannel);
                  return {
                    occurrences: Stream.fromSubscription(subscription),
                    health: SubscriptionRef.changes(health),
                  };
                })
              : Effect.fail(options.otherSubscribeError),
        },
      ] satisfies ReadonlyArray<EventType>,
    },
  ];
  const catalog = await Effect.runPromise(makeCatalog(sources));
  const scope = await Effect.runPromise(Scope.make());
  const upstream = await Effect.runPromise(
    makeUpstream(catalog).pipe(Effect.provideService(Scope.Scope, scope)),
  );
  const server = makeServer({
    catalog: catalog satisfies Catalog,
    upstream: upstream satisfies Upstream,
    defaultExpiresInSeconds: options.defaultExpiresInSeconds ?? 60,
    decide: options.decide,
    eventTimeoutSeconds: options.eventTimeoutSeconds ?? 30,
  });
  const client = new Client({ name: "test", version: "1" });
  const notifications: Array<{
    type: string;
    id: string | number;
    eventId?: string;
    name?: string;
    data?: unknown;
    timestamp?: string;
    reason?: string;
  }> = [];
  const params = Schema.toStandardSchemaV1(
    Schema.Struct({
      cursor: Schema.Null,
      _meta: Schema.Struct({
        [SUBSCRIPTION_ID_META_KEY]: Schema.Union([Schema.String, Schema.Number]),
      }),
      eventId: Schema.optionalKey(Schema.String),
      name: Schema.optionalKey(Schema.String),
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
        ...(value.name !== undefined && { name: value.name }),
        ...(value.timestamp !== undefined && { timestamp: value.timestamp }),
        ...(value.data !== undefined && { data: value.data }),
        ...(value.error && { reason: value.error.message }),
      });
    });
  }
  const [a, b] = InMemoryTransport.createLinkedPair();
  options.transport?.(b);
  await server.connect(b);
  await client.connect(a);
  const channels = { lark: larkChannel, other: otherChannel };
  const controllers: AbortController[] = [];
  const requests: Promise<unknown>[] = [];
  const result = Schema.toStandardSchemaV1(Schema.Unknown);
  return {
    client,
    notifications,
    stream(args: Record<string, unknown>, name: string = larkMessage) {
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
    publish(event: Publish, source: keyof typeof channels = "lark") {
      Effect.runSync(
        PubSub.publish(channels[source], {
          ...(event.eventId === undefined ? {} : { id: event.eventId }),
          occurredAt: DateTime.makeUnsafe(event.occurredAt ?? new Date().toISOString()),
          payload: event.payload,
        }),
      );
    },
    async call(name: string, args: Record<string, unknown> = {}) {
      const response = await client.request(
        { method: "tools/call", params: { name, arguments: args } },
        Schema.toStandardSchemaV1(
          Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
        ),
      );
      return JSON.parse(response.content[0]?.text ?? "null");
    },
    async callRaw(name: string, args: Record<string, unknown> = {}) {
      const response = await client.request(
        { method: "tools/call", params: { name, arguments: args } },
        Schema.toStandardSchemaV1(
          Schema.Struct({
            content: Schema.Array(Schema.Struct({ text: Schema.String })),
            isError: Schema.optionalKey(Schema.Boolean),
          }),
        ),
      );
      return { text: response.content[0]?.text ?? "", isError: response.isError === true };
    },
    async close() {
      for (const controller of controllers) controller.abort();
      await Promise.all(requests);
      await client.close();
      await server.close();
      await Effect.runPromise(PubSub.shutdown(larkChannel));
      await Effect.runPromise(PubSub.shutdown(otherChannel));
      await Effect.runPromise(Scope.close(scope, Exit.void));
    },
  };
}
