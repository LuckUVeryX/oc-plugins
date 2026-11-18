import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { connect } from "./harness.ts";

const name = "im.message.receive_v1";

test("fixed heartbeats survive event traffic and stop on cancellation or termination", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const terminalStarted = Promise.withResolvers<void>();
  const releaseTerminal = Promise.withResolvers<void>();
  const h = await connect({
    maxSeconds: 3600,
    transport(transport) {
      const send = transport.send.bind(transport);
      transport.send = async (message, options) => {
        if ("method" in message && message.method === "notifications/events/terminated") {
          terminalStarted.resolve();
          await releaseTerminal.promise;
        }
        return send(message, options);
      };
    },
  });
  try {
    const listing = await h.client.request(
      { method: "events/list", params: {} },
      Schema.toStandardSchemaV1(
        Schema.Struct({
          events: Schema.Array(
            Schema.Struct({
              name: Schema.String,
              inputSchema: Schema.Unknown,
              payloadSchema: Schema.Unknown,
            }),
          ),
        }),
      ),
    );
    const definition = listing.events.find((event) => event.name === name);
    expect(definition?.inputSchema).toHaveProperty([
      "properties",
      "filter",
      "properties",
      "message.chat_id",
    ]);
    expect(definition?.inputSchema).not.toHaveProperty(["properties", "debounce"]);
    expect(definition?.inputSchema).not.toHaveProperty(["properties", "durationSeconds"]);
    expect(definition?.payloadSchema).toEqual({
      anyOf: [h.catalog.get(name)?.payloadSchema, { type: "string" }],
    });
    const invalid = h.stream({ watchId: "invalid", debounce: {} });
    await vi.advanceTimersByTimeAsync(0);
    expect(await invalid.done).toMatchObject({ code: -32602 });
    const first = h.stream({ watchId: "heartbeat-1", filter: { "message.chat_id": "oc_test" } });
    h.stream({ watchId: "heartbeat-2", filter: { "message.chat_id": "oc_test" } });
    await vi.advanceTimersByTimeAsync(0);
    const active = h.notifications.filter((n) => n.type === "active");
    expect(active).toHaveLength(2);
    const heartbeats = () => h.notifications.filter((n) => n.type === "heartbeat");
    await vi.advanceTimersByTimeAsync(19_999);
    expect(heartbeats()).toEqual([]);
    await vi.advanceTimersByTimeAsync(1);
    expect(heartbeats()).toEqual(active.map((n) => ({ ...n, type: "heartbeat" })));
    await vi.advanceTimersByTimeAsync(10_000);
    await h.publish({ eventId: "ignored", payload: { message: { chat_id: "oc_other" } } });
    await h.publish({ eventId: "test", payload: { message: { chat_id: "oc_test" } } });
    await vi.advanceTimersByTimeAsync(0);
    expect(h.notifications.filter((n) => n.type === "event").map((n) => [n.id, n.data])).toEqual(
      active.map((n) => [n.id, { message: { chat_id: "oc_test" } }]),
    );
    await vi.advanceTimersByTimeAsync(10_000);
    expect(heartbeats()).toHaveLength(4);
    first.abort();
    await vi.advanceTimersByTimeAsync(20_000);
    expect(heartbeats()).toHaveLength(5);
    expect(heartbeats().at(-1)?.id).toBe(active[1]?.id);
    await h.send({ kind: "terminated", message: "test source closed" });
    await terminalStarted.promise;
    await vi.advanceTimersByTimeAsync(20_000);
    expect(heartbeats()).toHaveLength(5);
    releaseTerminal.resolve();
    await vi.advanceTimersByTimeAsync(0);
    expect(h.notifications.filter((n) => n.type === "terminated")).toEqual([
      { ...active[1], type: "terminated", reason: "test source closed" },
    ]);
  } finally {
    releaseTerminal.resolve();
    await h.close();
    vi.useRealTimers();
  }
});

test("watch controls and deadlines include connection readiness", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
  const h = await connect({
    maxSeconds: 1,
    ready: (eventName) => (eventName === name ? Effect.void : Effect.never),
  });
  try {
    h.stream({ watchId: "one" });
    await vi.advanceTimersByTimeAsync(0);
    expect((await h.call("list_watches")).watches).toEqual([
      {
        watchId: "one",
        eventName: name,
        startedAt: new Date().toISOString(),
        expiresAt: new Date(Date.now() + 1000).toISOString(),
      },
    ]);
    const duplicate = h.stream({ watchId: "one" });
    const invalid = h.stream({ watchId: "invalid", durationSeconds: 1 });
    await vi.advanceTimersByTimeAsync(0);
    expect(await duplicate.done).toMatchObject({ code: -32602 });
    expect(await invalid.done).toMatchObject({ code: -32602 });
    expect(await h.call("stop_watch", { watchId: "one" })).toEqual({
      watchId: "one",
      stopped: true,
    });
    expect(await h.call("stop_watch", { watchId: "one" })).toEqual({
      watchId: "one",
      stopped: false,
    });
    h.stream({ watchId: "one" });
    await vi.advanceTimersByTimeAsync(0);
    const expiry = (await h.call("list_watches")).watches[0].expiresAt;
    await vi.advanceTimersByTimeAsync(500);
    await h.publish({ eventId: "original", payload: { hello: "world" } });
    await vi.advanceTimersByTimeAsync(0);
    expect((await h.call("list_watches")).watches[0].expiresAt).toBe(expiry);
    await vi.advanceTimersByTimeAsync(500);
    expect(h.notifications.filter((n) => n.type === "event").map((n) => n.data)).toEqual([
      { hello: "world" },
    ]);
    expect(h.notifications.filter((n) => n.reason === "Expired")).toHaveLength(1);
    expect((await h.call("list_watches")).watches).toEqual([]);
    const waitingName = h.catalog.events.find((entry) => entry.name !== name)!.name;
    h.stream({ watchId: "waiting" }, waitingName);
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(1000);
    expect(h.notifications.filter((n) => n.reason === "Expired")).toHaveLength(2);
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.notifications.filter((n) => n.type === "heartbeat")).toEqual([]);
    expect((await h.call("list_watches")).watches).toEqual([]);
  } finally {
    await h.close();
    vi.useRealTimers();
  }
});

test("stop_watch returns only after the watch has stopped", async () => {
  const held = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const h = await connect({
    transport: (transport) => {
      const send = transport.send.bind(transport);
      transport.send = async (message, options) => {
        if ("method" in message && message.method === "notifications/events/terminated") {
          held.resolve();
          await release.promise;
        }
        return send(message, options);
      };
    },
  });
  try {
    h.stream({ watchId: "held" });
    await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "active")).toBe(true));
    let stopped = false;
    const stop = h.call("stop_watch", { watchId: "held" }).then((result) => {
      stopped = true;
      return result;
    });
    await held.promise;
    await new Promise((resolve) => setImmediate(resolve));
    expect(stopped).toBe(false);
    release.resolve();
    expect(await stop).toEqual({ watchId: "held", stopped: true });
  } finally {
    release.resolve();
    await h.close();
  }
});
