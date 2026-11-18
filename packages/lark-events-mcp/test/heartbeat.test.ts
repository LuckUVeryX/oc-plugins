import { expect, test, vi } from "vitest";
import { connect } from "./harness.ts";

test("heartbeats continue during event traffic and stop on cancellation", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const h = await connect({ maxSeconds: 3600 });
  try {
    const watch = h.stream({
      watchId: "heartbeat",
      prepare: "return input.event.data",
      transform: "return JSON.stringify(input.prepared)",
    });
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.notifications.filter((n) => n.type === "heartbeat")).toHaveLength(1);
    await h.publish({ eventId: "event", payload: {} });
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.notifications.filter((n) => n.type === "heartbeat")).toHaveLength(2);
    watch.abort();
    await vi.advanceTimersByTimeAsync(20_000);
    expect(h.notifications.filter((n) => n.type === "heartbeat")).toHaveLength(2);
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
    h.stream({
      prepare: "return input.event.data",
      transform: "return JSON.stringify(input.prepared)",
      watchId: "held",
    });
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
