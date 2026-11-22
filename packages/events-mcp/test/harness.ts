import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport, SUBSCRIPTION_ID_META_KEY } from "@modelcontextprotocol/server";
import {
  DateTime,
  Effect,
  Exit,
  Layer,
  PubSub,
  Schema,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { Registry } from "../src/core/registry.ts";
import { DecisionModel } from "../src/core/decision-model.ts";
import { Server } from "../src/core/server.ts";
import {
  Health,
  IntegrationName,
  type EventKind,
  type Event,
  type Integration,
  type IntegrationError,
} from "../src/core/integration.ts";
import { Feeds } from "../src/core/feeds.ts";
import { makeGetTool, type LarkRequester } from "../src/lark/get.ts";

const larkApp = { appId: "cli_0123456789abcdef", appSecret: "offline" };
const lark = Schema.decodeUnknownSync(IntegrationName)("lark");
const other = Schema.decodeUnknownSync(IntegrationName)("other");
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

// A watch that only needs the decide tool to exist, not to answer.
export const unusedDecide: DecisionModel.Decide = () =>
  Effect.fail(new DecisionModel.Failed({ message: "unused" }));

interface Publish {
  readonly eventId?: string;
  readonly occurredAt?: string;
  readonly payload: Schema.JsonObject;
}

export async function connect(
  options: {
    defaultExpiresInSeconds?: number;
    decide?: DecisionModel.Decide | undefined;
    eventTimeoutSeconds?: number;
    request?: LarkRequester;
    otherSubscribeError?: IntegrationError | undefined;
    integrations?: ReadonlyArray<Integration>;
    transport?: (transport: ReturnType<typeof InMemoryTransport.createLinkedPair>[1]) => void;
  } = {},
) {
  const request: LarkRequester =
    options.request ?? (async () => ({ code: 0, msg: "ok", data: null }));
  const larkChannel = await Effect.runPromise(PubSub.unbounded<Event>());
  const otherChannel = await Effect.runPromise(PubSub.unbounded<Event>());
  const health = await Effect.runPromise(SubscriptionRef.make<Health>(Health.Live()));
  const integrations: ReadonlyArray<Integration> = options.integrations ?? [
    {
      name: lark,
      tools: {
        get: makeGetTool({ credentials: () => Effect.succeed({ ...larkApp, request }) }),
      },
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
              const pubsub = yield* PubSub.subscribe(larkChannel);
              return {
                events: Stream.fromSubscription(pubsub),
                health: SubscriptionRef.changes(health),
              };
            }),
        },
      ] satisfies ReadonlyArray<EventKind>,
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
                  const pubsub = yield* PubSub.subscribe(otherChannel);
                  return {
                    events: Stream.fromSubscription(pubsub),
                    health: SubscriptionRef.changes(health),
                  };
                })
              : Effect.fail(options.otherSubscribeError),
        },
      ] satisfies ReadonlyArray<EventKind>,
    },
  ];
  const registryLayer = Registry.layer(Effect.succeed(integrations));
  const decisionModelLayer = Layer.succeed(
    DecisionModel.Service,
    DecisionModel.Service.of({ decide: options.decide ?? unusedDecide }),
  );
  const scope = await Effect.runPromise(Scope.make());
  const context = await Effect.runPromise(
    Layer.build(
      Layer.merge(Feeds.layer.pipe(Layer.provideMerge(registryLayer)), decisionModelLayer),
    ).pipe(Effect.provideService(Scope.Scope, scope)),
  );
  const server = await Effect.runPromise(
    Server.make({
      defaultExpiresInSeconds: options.defaultExpiresInSeconds ?? 60,
      eventTimeoutSeconds: options.eventTimeoutSeconds ?? 30,
    }).pipe(Effect.provide(context)),
  );
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
    publish(event: Publish, integration: keyof typeof channels = "lark") {
      Effect.runSync(
        PubSub.publish(channels[integration], {
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
