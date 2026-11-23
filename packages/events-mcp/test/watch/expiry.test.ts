import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

test("a watch stays open until its chosen expiry", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const h = await connect({ harness: "claude", defaultExpiresInSeconds: 3600 });
  const open = async () =>
    (await h.call("watches")).watches.map((watch: { id: string }) => watch.id);
  try {
    const watch = { event: "lark.im.message.receive_v1", code: "return null" };
    await h.call("watch", { ...watch, id: "long", expiresInSeconds: 28_800 });
    await h.call("watch", { ...watch, id: "standard" });
    await vi.advanceTimersByTimeAsync(2 * 3_600_000);
    expect(await open()).toEqual(["long"]);
    await vi.advanceTimersByTimeAsync(6 * 3_600_000);
    expect(await open()).toEqual([]);
  } finally {
    await h.close();
    vi.useRealTimers();
  }
});

test("OpenCode owns a watch's lifetime", async () => {
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  const h = await connect({ defaultExpiresInSeconds: 3600 });
  const ofType = (type: string) => h.notifications.filter((n) => n.type === type);
  try {
    h.stream({});
    await vi.waitFor(() => expect(ofType("active")).toHaveLength(1));
    await vi.advanceTimersByTimeAsync(8 * 3_600_000);
    expect(ofType("terminated")).toEqual([]);
  } finally {
    await h.close();
    vi.useRealTimers();
  }
});

test("OpenCode passes a target and gets no second way to filter, name, expire or stop a watch", async () => {
  const h = await connect();
  try {
    expect((await h.client.listTools()).tools.map((tool) => tool.name)).toEqual([
      "lark_get",
      "decide",
    ]);
    for (const extra of [{ code: "return null" }, { expiresInSeconds: 60 }, { id: "named" }])
      expect(await h.stream(extra).done).toMatchObject({ code: -32602 });
  } finally {
    await h.close();
  }
});
