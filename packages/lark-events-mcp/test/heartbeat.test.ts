import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport, SUBSCRIPTION_ID_META_KEY } from "@modelcontextprotocol/server";
import { Effect, PubSub, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { makeServer } from "../src/server.ts";
import { catalogLayer, LarkCatalog } from "../src/catalog.ts";
import type { SourceMessage } from "../src/producers.ts";
const FLAT_KEY = "im.message.receive_v1";

test("fixed heartbeats survive event traffic and stop on cancellation or termination", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const messages = await Effect.runPromise(PubSub.unbounded<SourceMessage>());
  const catalog = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* LarkCatalog;
    }).pipe(Effect.provide(catalogLayer)),
  );
  const server = makeServer({
    catalog,
    producers: {
      acquire: () =>
        Effect.succeed({
          messages,
          ready: Effect.void,
          terminal: Effect.succeed(undefined),
        }),
    },
  });
  const client = new Client({ name: "heartbeat-test", version: "0.0.0" });
  const notifications: Array<{
    method: string;
    cursor: null;
    id: string | number;
    reason?: string;
    data?: unknown;
  }> = [];
  const params = Schema.toStandardSchemaV1(
    Schema.Struct({
      cursor: Schema.Null,
      error: Schema.optionalKey(Schema.Struct({ code: Schema.Number, message: Schema.String })),
      data: Schema.optionalKey(Schema.Unknown),
      _meta: Schema.Struct({
        [SUBSCRIPTION_ID_META_KEY]: Schema.Union([Schema.String, Schema.Number]),
      }),
    }),
  );
  for (const type of ["active", "heartbeat", "event", "terminated"]) {
    client.setNotificationHandler(`notifications/events/${type}`, { params }, (value) => {
      notifications.push({
        method: type,
        cursor: value.cursor,
        id: value._meta[SUBSCRIPTION_ID_META_KEY],
        ...(value.error ? { reason: value.error.message } : {}),
        ...(value.data ? { data: value.data } : {}),
      });
    });
  }
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  const terminalStarted = Promise.withResolvers<void>();
  const releaseTerminal = Promise.withResolvers<void>();
  const send = serverTransport.send.bind(serverTransport);
  serverTransport.send = async (message, options) => {
    if ("method" in message && message.method === "notifications/events/terminated") {
      terminalStarted.resolve();
      await releaseTerminal.promise;
    }
    return send(message, options);
  };
  const controllers: AbortController[] = [];
  const requests: Promise<unknown>[] = [];
  const result = Schema.toStandardSchemaV1(Schema.Unknown);
  const subscribe = () => {
    const controller = new AbortController();
    controllers.push(controller);
    requests.push(
      client
        .request(
          {
            method: "events/stream",
            params: {
              name: FLAT_KEY,
              arguments: {
                watchId: `heartbeat-${controllers.length}`,
                filter: { "message.chat_id": "oc_test" },
              },
            },
          },
          result,
          {
            signal: controller.signal,
            timeout: 120_000,
          },
        )
        .catch((error: unknown) => error),
    );
    return controller;
  };
  const heartbeats = () => notifications.filter((value) => value.method === "heartbeat");
  try {
    await server.connect(serverTransport);
    await client.connect(clientTransport);
    const listing = await client.request(
      { method: "events/list", params: {} },
      Schema.toStandardSchemaV1(
        Schema.Struct({
          events: Schema.Array(Schema.Struct({ name: Schema.String, inputSchema: Schema.Unknown })),
        }),
      ),
    );
    expect(listing.events.find((event) => event.name === FLAT_KEY)?.inputSchema).toHaveProperty([
      "properties",
      "filter",
      "properties",
      "message.chat_id",
    ]);
    const first = subscribe();
    subscribe();
    await vi.advanceTimersByTimeAsync(0);
    const active = notifications.filter((value) => value.method === "active");
    expect(active).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(19_999);
    expect(heartbeats()).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(heartbeats()).toEqual(active.map((value) => ({ ...value, method: "heartbeat" })));
    await vi.advanceTimersByTimeAsync(10_000);
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        payload: { message: { chat_id: "oc_other" } },
        eventId: "ignored-event",
        timestamp: new Date(0).toISOString(),
        timestampOrigin: "local",
      }),
    );
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        payload: { message: { chat_id: "oc_test" } },
        eventId: "test-event",
        timestamp: new Date(0).toISOString(),
        timestampOrigin: "local",
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(notifications.filter((value) => value.method === "event")).toEqual(
      active.map((value) => ({
        ...value,
        method: "event",
        data: { message: { chat_id: "oc_test" } },
      })),
    );
    await vi.advanceTimersByTimeAsync(10_000);
    expect(heartbeats()).toHaveLength(4);
    first.abort();
    await vi.advanceTimersByTimeAsync(20_000);
    expect(heartbeats()).toHaveLength(5);
    expect(heartbeats().at(-1)?.id).toBe(active[1]?.id);
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "terminated",
        message: "test source closed",
      }),
    );
    await terminalStarted.promise;
    await vi.advanceTimersByTimeAsync(20_000);
    expect(heartbeats()).toHaveLength(5);
    releaseTerminal.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(notifications.filter((value) => value.method === "terminated")).toEqual([
      { ...active[1], method: "terminated", reason: "test source closed" },
    ]);
  } finally {
    releaseTerminal.resolve();
    for (const controller of controllers) controller.abort();
    await Promise.all(requests);
    await client.close();
    await server.close();
    await Effect.runPromise(PubSub.shutdown(messages));
    vi.useRealTimers();
  }
});

test("watch controls and deadlines include connection readiness", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const messages = await Effect.runPromise(PubSub.unbounded<SourceMessage>());
  const catalog = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* LarkCatalog;
    }).pipe(Effect.provide(catalogLayer)),
  );
  const server = makeServer({
    catalog,
    producers: {
      acquire: (name) =>
        Effect.succeed({
          messages,
          ready: name === FLAT_KEY ? Effect.void : Effect.never,
          terminal: Effect.succeed(undefined),
        }),
    },
  });
  const client = new Client({ name: "controls-test", version: "0.0.0" });
  const received: Array<{ method: string; data?: unknown; reason?: string }> = [];
  const notificationParams = Schema.toStandardSchemaV1(
    Schema.Struct({
      data: Schema.optionalKey(Schema.Unknown),
      error: Schema.optionalKey(Schema.Struct({ message: Schema.String })),
    }),
  );
  for (const method of ["active", "event", "heartbeat", "terminated"]) {
    client.setNotificationHandler(
      `notifications/events/${method}`,
      { params: notificationParams },
      (value) => {
        received.push({
          method,
          ...(value.data ? { data: value.data } : {}),
          ...(value.error ? { reason: value.error.message } : {}),
        });
      },
    );
  }
  const [a, b] = InMemoryTransport.createLinkedPair();
  const result = Schema.toStandardSchemaV1(Schema.Unknown);
  const requests: Promise<unknown>[] = [];
  const controllers: AbortController[] = [];
  const subscribe = (watchId: string, name = FLAT_KEY, durationSeconds = 60) => {
    const controller = new AbortController();
    controllers.push(controller);
    const request = client
      .request(
        { method: "events/stream", params: { name, arguments: { watchId, durationSeconds } } },
        result,
        { signal: controller.signal },
      )
      .catch((error: unknown) => error);
    requests.push(request);
    return request;
  };
  const tool = async (name: string, args: Record<string, unknown> = {}) => {
    const response = await client.request(
      { method: "tools/call", params: { name, arguments: args } },
      Schema.toStandardSchemaV1(
        Schema.Struct({ content: Schema.Array(Schema.Struct({ text: Schema.String })) }),
      ),
    );
    return JSON.parse(response.content[0]!.text);
  };
  try {
    await server.connect(b);
    await client.connect(a);
    subscribe("one");
    await vi.advanceTimersByTimeAsync(0);
    const listed = await tool("watch_list");
    expect(listed.watches).toEqual([
      {
        watchId: "one",
        eventName: FLAT_KEY,
        startedAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 60_000).toISOString(),
      },
    ]);
    const duplicate = subscribe("one");
    await vi.advanceTimersByTimeAsync(0);
    expect(await duplicate).toMatchObject({ code: -32602 });
    const invalid = client
      .request(
        {
          method: "events/stream",
          params: { name: FLAT_KEY, arguments: { watchId: "invalid", durationSeconds: 1.5 } },
        },
        result,
      )
      .catch((error: unknown) => error);
    await vi.advanceTimersByTimeAsync(0);
    expect(await invalid).toMatchObject({ code: -32602 });
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        eventId: "original",
        timestamp: new Date(0).toISOString(),
        timestampOrigin: "upstream",
        payload: { hello: "world" },
      }),
    );
    await vi.advanceTimersByTimeAsync(0);
    expect(received).toContainEqual({ method: "event", data: { hello: "world" } });
    expect(await tool("watch_stop", { watchId: "one" })).toEqual({ watchId: "one", stopped: true });
    expect(await tool("watch_stop", { watchId: "one" })).toEqual({
      watchId: "one",
      stopped: false,
    });
    subscribe("one", FLAT_KEY, 1);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(received.filter((value) => value.reason === "Expired")).toHaveLength(1);
    expect((await tool("watch_list")).watches).toEqual([]);
    const waitingName = (await Effect.runPromise(catalog.list())).find(
      (entry) => entry.name !== FLAT_KEY,
    )!.name;
    subscribe("waiting", waitingName, 1);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(received.filter((value) => value.reason === "Expired")).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(received.filter((value) => value.method === "heartbeat")).toEqual([]);
    expect((await tool("watch_list")).watches).toEqual([]);
  } finally {
    for (const controller of controllers) controller.abort();
    await Promise.all(requests);
    await client.close();
    await server.close();
    await Effect.runPromise(PubSub.shutdown(messages));
    vi.useRealTimers();
  }
});
