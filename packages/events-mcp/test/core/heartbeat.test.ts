import { expect, test, vi } from "vitest";
import { connect } from "../harness.ts";

// OpenCode cancels a feed after 60 seconds without a notification.
test("heartbeats continue during event traffic and stop on cancellation", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const h = await connect({ defaultExpiresInSeconds: 3600 });
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
