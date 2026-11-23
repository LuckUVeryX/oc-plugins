import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

// OpenCode ends a stream after three granted heartbeats pass without a notification.
test("heartbeats continue during event traffic and stop on cancellation", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const h = await connect({ defaultExpiresInSeconds: 3600 });
  try {
    const watch = h.stream({});
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
