import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport } from "@modelcontextprotocol/server";
import { Effect, Layer, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { catalogLayer, LarkCatalog } from "../src/catalog.ts";
import { EventProducers, producersLayer } from "../src/producers.ts";
import { makeServer } from "../src/server.ts";

const sdk = vi.hoisted(() => ({
  dispatcher: undefined as undefined | import("@larksuiteoapi/node-sdk").EventDispatcher,
}));
vi.mock("@larksuiteoapi/node-sdk", async (importOriginal) => {
  const original = await importOriginal<typeof import("@larksuiteoapi/node-sdk")>();
  return {
    ...original,
    WSClient: class {
      constructor(private options: { onReady: () => void }) {}
      async start({ eventDispatcher }: { eventDispatcher: typeof sdk.dispatcher }) {
        sdk.dispatcher = eventDispatcher;
        this.options.onReady();
      }
      close() {}
    },
  };
});

test("delivers fresh events once and drops stale events", async () => {
  vi.stubEnv("LARK_APP_ID", "cli_0123456789abcdef");
  vi.stubEnv("LARK_APP_SECRET", "offline");
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const catalog = yield* LarkCatalog;
          const producers = yield* EventProducers;
          yield* Effect.promise(async () => {
            const server = makeServer({ catalog, producers });
            const client = new Client({ name: "dedup-test", version: "1" });
            const active = Promise.withResolvers<void>();
            const received: unknown[] = [];
            client.setNotificationHandler(
              "notifications/events/active",
              { params: Schema.toStandardSchemaV1(Schema.Unknown) },
              () => {
                active.resolve();
              },
            );
            client.setNotificationHandler(
              "notifications/events/event",
              {
                params: Schema.toStandardSchemaV1(
                  Schema.Struct({
                    data: Schema.Unknown,
                    _meta: Schema.Record(Schema.String, Schema.Unknown),
                  }),
                ),
              },
              (value) => {
                received.push(value);
              },
            );
            const [a, b] = InMemoryTransport.createLinkedPair();
            const controller = new AbortController();
            await server.connect(b);
            await client.connect(a);
            const listing = await client.request(
              { method: "events/list", params: {} },
              Schema.toStandardSchemaV1(
                Schema.Struct({ events: Schema.Array(Schema.Struct({ name: Schema.String })) }),
              ),
            );
            expect(listing.events.map((event) => event.name)).toContain("im.message.receive_v1");
            expect(listing.events.map((event) => event.name)).not.toContain("order_paid");
            await expect(
              client.request(
                { method: "events/stream", params: { name: "order_paid" } },
                Schema.toStandardSchemaV1(Schema.Unknown),
              ),
            ).rejects.toMatchObject({ code: -32602 });
            const watch = (name: string, watchId: string) =>
              client
                .request(
                  {
                    method: "events/stream",
                    params: {
                      name,
                      arguments: { watchId, debounce: { waitSeconds: 0 } },
                    },
                  },
                  Schema.toStandardSchemaV1(Schema.Unknown),
                  { signal: controller.signal },
                )
                .catch(() => {});
            const requests = [watch("im.message.receive_v1", "dedup")];
            try {
              await active.promise;
              const dispatcher = sdk.dispatcher;
              if (!dispatcher) throw new Error("SDK dispatcher was not started");
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
              await vi.waitFor(() => {
                expect(JSON.stringify(received)).toContain("fresh-sentinel");
              });
              expect(received).toEqual(
                expect.arrayContaining([
                  expect.objectContaining({
                    data: expect.objectContaining({
                      events: [
                        expect.objectContaining({
                          eventId: "same",
                          data: expect.objectContaining({
                            message: { chat_id: "oc_test", message_id: "same" },
                          }),
                        }),
                      ],
                    }),
                  }),
                  expect.objectContaining({
                    data: expect.objectContaining({
                      events: [
                        expect.objectContaining({
                          eventId: "fresh-sentinel",
                          data: expect.objectContaining({
                            message: { chat_id: "oc_test", message_id: "fresh-sentinel" },
                          }),
                        }),
                      ],
                    }),
                  }),
                ]),
              );
              expect(received).toHaveLength(2);
            } finally {
              controller.abort();
              await Promise.all(requests);
              await client.close();
              await server.close();
            }
          });
        }).pipe(Effect.provide(Layer.mergeAll(catalogLayer, producersLayer))),
      ),
    );
  } finally {
    vi.unstubAllEnvs();
  }
});
