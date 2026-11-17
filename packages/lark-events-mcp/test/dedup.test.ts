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

test("duplicate IDs produce one notification while distinct old events are delivered", async () => {
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
              () => active.resolve(),
            );
            client.setNotificationHandler(
              "notifications/events/event",
              { params: Schema.toStandardSchemaV1(Schema.Struct({ data: Schema.Unknown })) },
              (value) => {
                received.push(value.data);
              },
            );
            const [a, b] = InMemoryTransport.createLinkedPair();
            const controller = new AbortController();
            await server.connect(b);
            await client.connect(a);
            const request = client
              .request(
                {
                  method: "events/stream",
                  params: {
                    name: "im.message.receive_v1",
                    arguments: { watchId: "dedup" },
                  },
                },
                Schema.toStandardSchemaV1(Schema.Unknown),
                { signal: controller.signal },
              )
              .catch(() => {});
            try {
              await active.promise;
              const dispatcher = sdk.dispatcher;
              if (!dispatcher) throw new Error("SDK dispatcher was not started");
              const dispatch = (id: string) =>
                dispatcher.invoke(
                  {
                    schema: "2.0",
                    header: {
                      event_id: id,
                      event_type: "im.message.receive_v1",
                      create_time: "946684800000",
                    },
                    event: { message: { chat_id: "oc_test", message_id: id } },
                  },
                  { needCheck: false },
                );
              await dispatch("same");
              await dispatch("same");
              await dispatch("distinct-old");
              await vi.waitFor(() => expect(JSON.stringify(received)).toContain("distinct-old"));
              expect(received).toMatchObject([
                { message: { chat_id: "oc_test", message_id: "same" } },
                { message: { chat_id: "oc_test", message_id: "distinct-old" } },
              ]);
              expect(received).toHaveLength(2);
            } finally {
              controller.abort();
              await request;
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
