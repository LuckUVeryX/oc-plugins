import { ConfigProvider, Effect, Fiber, Layer, Schema, type Scope } from "effect";
import { expect, test, vi } from "vitest";
import { LarkAuth } from "../../src/lark/auth.ts";
import { LarkSocket } from "../../src/lark/socket.ts";
import { Lark } from "../../src/lark/integration.ts";
import type { LarkRequester } from "../../src/lark/get.ts";
import type { Integration } from "../../src/integration/contract.ts";
import { connect } from "../harness.ts";

const sdk = vi.hoisted(() => ({
  dispatcher: undefined as undefined | import("@larksuiteoapi/node-sdk").EventDispatcher,
  starts: 0,
  closes: 0,
  silent: false,
}));
vi.mock("@larksuiteoapi/node-sdk", async (importOriginal) => {
  const original = await importOriginal<typeof import("@larksuiteoapi/node-sdk")>();
  return {
    ...original,
    WSClient: class {
      constructor(private options: { onReady: () => void }) {}
      async start({ eventDispatcher }: { eventDispatcher: typeof sdk.dispatcher }) {
        sdk.starts++;
        sdk.dispatcher = eventDispatcher;
        if (sdk.silent) return new Promise(() => {});
        this.options.onReady();
      }
      close() {
        sdk.closes++;
      }
    },
  };
});

const request = async () => ({ code: 0, msg: "", data: { online_instance_cnt: 0 } });

const fakeApi = (requester: LarkRequester) =>
  Layer.succeed(
    LarkAuth.Service,
    LarkAuth.Service.of({
      credentials: () =>
        Effect.succeed({
          appId: "cli_0123456789abcdef",
          appSecret: "offline",
          request: requester,
        }),
    }),
  );

const integrationWith = (
  requester: LarkRequester,
): Effect.Effect<Integration, never, Scope.Scope> =>
  Effect.gen(function* () {
    const context = yield* Layer.build(
      LarkSocket.layer.pipe(Layer.provideMerge(fakeApi(requester))),
    );
    return yield* Lark.integration.pipe(Effect.provide(context));
  });

const withLark = <A, E>(
  body: (integration: Integration) => Effect.Effect<A, E, Scope.Scope>,
  options: { readonly request?: LarkRequester } = {},
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const integration = yield* integrationWith(options.request ?? request);
        return yield* body(integration);
      }),
    ),
  );

const messageEvent = (integration: Integration) => {
  const event = integration.events.find((item) => item.name === "im.message.receive_v1");
  if (event === undefined) throw new Error("Lark message event is missing");
  return event;
};

const Events = Schema.Struct({
  events: Schema.Array(Schema.Struct({ name: Schema.String })),
});

test("delivers fresh events once and drops stale events", async () => {
  sdk.silent = false;
  await withLark((integration) =>
    Effect.promise(async () => {
      const h = await connect({ integrations: [integration] });
      try {
        const listing = await h.client.request(
          { method: "events/list", params: {} },
          Schema.toStandardSchemaV1(Events),
        );
        expect(listing.events).toHaveLength(14);
        expect(listing.events.map((event) => event.name)).toContain("lark.im.message.receive_v1");
        expect(listing.events.map((event) => event.name)).not.toContain("order_paid");
        expect(
          await h.stream(
            {
              watchId: "dedup",
              script: "return input.event.data",
            },
            "order_paid",
          ).done,
        ).toMatchObject({
          code: -32602,
        });
        h.stream(
          {
            watchId: "dedup",
            script: "return input.event.data",
          },
          "lark.im.message.receive_v1",
        );
        await vi.waitFor(() =>
          expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(1),
        );
        const dispatcher = sdk.dispatcher;
        if (dispatcher === undefined) throw new Error("SDK dispatcher was not started");
        const dispatch = (id: string, time: string) =>
          dispatcher.invoke(
            {
              schema: "2.0",
              header: {
                event_id: id,
                event_type: "im.message.receive_v1",
                create_time: time,
              },
              event: { message: { chat_id: "oc_test", message_id: id } },
            },
            { needCheck: false },
          );
        const recent = String(Date.now());
        await dispatch("same", recent);
        await dispatch("same", recent);
        await dispatch("distinct-old", "946684800000");
        await dispatch("fresh-sentinel", recent);
        await vi.waitFor(() =>
          expect(h.notifications.some((n) => n.eventId === "fresh-sentinel")).toBe(true),
        );
        expect(h.notifications.filter((n) => n.type === "event")).toEqual([
          expect.objectContaining({
            eventId: "same",
            timestamp: new Date(Number(recent)).toISOString(),
            data: expect.stringContaining('"message_id":"same"'),
          }),
          expect.objectContaining({
            eventId: "fresh-sentinel",
            timestamp: new Date(Number(recent)).toISOString(),
            data: expect.stringContaining('"message_id":"fresh-sentinel"'),
          }),
        ]);
      } finally {
        await h.close();
      }
    }),
  );
});

test("closes the socket when an interrupted watch is still waiting for readiness", async () => {
  sdk.starts = 0;
  sdk.closes = 0;
  sdk.silent = true;
  const fiber = Effect.runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const integration = yield* integrationWith(request);
        yield* messageEvent(integration).subscribe({});
      }),
    ),
  );
  try {
    await vi.waitFor(() => expect(sdk.starts).toBe(1));
    await Effect.runPromise(Fiber.interrupt(fiber));
    expect(sdk.closes).toBe(1);
  } finally {
    sdk.silent = false;
    await Effect.runPromise(Fiber.interrupt(fiber));
  }
});

test("refuses to connect while another app connection is online", async () => {
  sdk.starts = 0;
  sdk.silent = false;
  await withLark(
    (integration) =>
      messageEvent(integration)
        .subscribe({})
        .pipe(
          Effect.flip,
          Effect.tap((error) =>
            Effect.sync(() => {
              expect(error).toMatchObject({ _tag: "Integration.Unavailable" });
              expect(error.message).toContain("Another connection for this Lark app is online (1)");
              expect(error.message).toContain("Lark delivers each event to only one connection");
              expect(sdk.starts).toBe(0);
            }),
          ),
        ),
    { request: async () => ({ code: 0, msg: "", data: { online_instance_cnt: 1 } }) },
  );
});

test("a watch without Lark credentials is rejected and names both variables", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(
          LarkSocket.layer.pipe(Layer.provideMerge(LarkAuth.layer)),
        );
        const integration = yield* Lark.integration.pipe(Effect.provide(context));
        const error = yield* messageEvent(integration).subscribe({}).pipe(Effect.flip);
        expect(error).toMatchObject({ _tag: "Integration.Rejected" });
        expect(error.message).toContain("LARK_APP_ID");
        expect(error.message).toContain("LARK_APP_SECRET");
      }).pipe(Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({})))),
    ),
  );
});
