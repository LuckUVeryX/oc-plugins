import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

test("keeps per-watch state between events and rejects state over 64 KB", async () => {
  const h = await connect({ harness: "claude" });
  try {
    await h.watch(
      "memory",
      "const previous = await tools.watch.state.get(); await tools.watch.state.set((previous ?? 0) + 1); return { previous }",
    );
    h.publish({ eventId: "first", payload: {} });
    h.publish({ eventId: "second", payload: {} });
    await vi.waitFor(() => expect(h.channel).toHaveLength(2));
    expect(h.channel.map((wake) => JSON.parse(wake.content))).toEqual([
      { previous: null },
      { previous: 1 },
    ]);

    await h.watch(
      "too-big",
      'await tools.watch.state.set({ text: "x".repeat(70000) }); return null',
    );
    h.publish({ eventId: "oversized", payload: {} });
    await vi.waitFor(() =>
      expect(h.channel.some((wake) => wake.content.includes("65536"))).toBe(true),
    );
    expect(h.channel.find((wake) => wake.content.includes("65536"))?.content).toContain(
      "code failed on oversized:",
    );
  } finally {
    await h.close();
  }
});
