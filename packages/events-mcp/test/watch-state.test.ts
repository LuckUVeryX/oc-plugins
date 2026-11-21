import { expect, test, vi } from "vitest";
import { connect } from "./harness.ts";

const active = async (h: Awaited<ReturnType<typeof connect>>, count = 1) => {
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(count),
  );
};

test("keeps per-watch state between events and rejects state over 64 KB", async () => {
  const h = await connect();
  try {
    h.stream({
      watchId: "memory",
      prepare:
        "const previous = await tools.watch.state.get(); await tools.watch.state.set((previous ?? 0) + 1); return { previous }",
      transform: "return JSON.stringify(input.prepared)",
    });
    await active(h);
    await h.publish({ eventId: "first", payload: {} });
    await h.publish({ eventId: "second", payload: {} });
    await vi.waitFor(() =>
      expect(h.notifications.filter((n) => n.eventId === "second")).toHaveLength(1),
    );
    expect(
      h.notifications.filter((n) => n.type === "event").map((n) => [n.eventId, n.data]),
    ).toEqual([
      ["first", '{"previous":null}'],
      ["second", '{"previous":1}'],
    ]);

    h.stream({
      watchId: "too-big",
      prepare: 'await tools.watch.state.set({ text: "x".repeat(70000) }); return null',
      transform: "return null",
    });
    await active(h, 2);
    await h.publish({ eventId: "oversized", payload: {} });
    await vi.waitFor(() =>
      expect(
        h.notifications.some((n) => n.type === "terminated" && n.reason?.includes("65536")),
      ).toBe(true),
    );
    expect(h.notifications.find((n) => n.type === "terminated")?.reason).toContain(
      "prepare failed on oversized:",
    );
  } finally {
    await h.close();
  }
});
