import { expect, test, vi } from "vitest";
import { connect } from "./harness.ts";

// OpenCode measures the delivered event as { id, timestamp, data } and rejects one above 16,384 bytes.
const timestamp = new Date().toISOString();
const overhead = Buffer.byteLength(JSON.stringify({ id: "size", timestamp, data: "" }));

test("stops a watch whose delivered event exceeds the OpenCode envelope", async () => {
  const h = await connect();
  try {
    const watch = (watchId: string, size: number) =>
      h.stream({
        watchId,
        prepare: "return input.event.data",
        transform: `return "x".repeat(${size})`,
      });
    watch("fits", 16_384 - overhead);
    watch("over", 16_385 - overhead);
    await vi.waitFor(() =>
      expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(2),
    );
    await h.publish({ eventId: "size", occurredAt: timestamp, payload: {} });
    await vi.waitFor(() =>
      expect(
        h.notifications.some(
          (n) => n.type === "terminated" && n.reason?.includes("exceeds 16384 bytes"),
        ),
      ).toBe(true),
    );
    const delivered = h.notifications.filter((n) => n.type === "event");
    expect(delivered).toHaveLength(1);
    expect(String(delivered[0]?.data)).toHaveLength(16_384 - overhead);
  } finally {
    await h.close();
  }
});
