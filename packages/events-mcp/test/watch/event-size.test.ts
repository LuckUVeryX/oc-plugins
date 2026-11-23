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
