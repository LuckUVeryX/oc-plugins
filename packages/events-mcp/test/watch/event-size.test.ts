import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

// The cap counts { id, timestamp, data } with data as the code returned it.
const timestamp = new Date().toISOString();
const overhead = Buffer.byteLength(JSON.stringify({ id: "size", timestamp, data: "" }));

test("stops a watch whose wake exceeds the cap", async () => {
  const h = await connect({ harness: "claude" });
  try {
    await h.watch("fits", `return "x".repeat(${16_384 - overhead})`);
    await h.watch("over", `return "x".repeat(${16_385 - overhead})`);
    h.publish({ eventId: "size", occurredAt: timestamp, payload: {} });
    await vi.waitFor(() => expect(h.channel).toHaveLength(2));
    const of = (id: string) => h.channel.find((wake) => wake.meta?.["watch_id"] === id)?.content;
    expect(of("fits")).toHaveLength(16_384 - overhead);
    expect(of("over")).toContain("exceeds 16384 bytes");
  } finally {
    await h.close();
  }
});

test("OpenCode gets a raw event of any size", async () => {
  const h = await connect();
  try {
    h.stream({});
    await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "active")).toBe(true));
    const payload = { text: "x".repeat(40_000) };
    h.publish({ eventId: "large", payload });
    await vi.waitFor(() =>
      expect(h.notifications.find((n) => n.type === "event")?.data).toEqual(payload),
    );
  } finally {
    await h.close();
  }
});
