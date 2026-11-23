import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { SourceName, type Source } from "../../src/source/contract.ts";
import { connect } from "../support.ts";

const blocked: Source = {
  name: Schema.decodeUnknownSync(SourceName)("blocked"),
  tools: {},
  events: [
    {
      name: "ping",
      description: "A ping.",
      payload: { type: "object" },
      subscribe: () => Effect.never,
    },
  ],
};

test(
  "a watch that expires before its feed is ready returns the reason",
  { timeout: 8_000 },
  async () => {
    const h = await connect({ harness: "claude", sources: [blocked] });
    try {
      const armed = await h.call("watch", {
        event: "blocked.ping",
        id: "expiring",
        code: "return null",
        expiresInSeconds: 1,
      });
      expect(armed).toEqual({ id: "expiring", ended: "Expired" });
      await vi.waitFor(() => expect(h.channel).toHaveLength(1));
      expect(h.channel[0]).toEqual({
        content: "Watch expiring ended: Expired",
        meta: { watch_id: "expiring", event: "blocked.ping" },
      });
    } finally {
      await h.close();
    }
  },
);

test("a failed watch wakes the session, and unwatch does not", async () => {
  const h = await connect({ harness: "claude" });
  try {
    await h.call("watch", {
      event: "lark.im.message.receive_v1",
      id: "failing",
      code: "throw new Error('missing secret')",
    });
    h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(1));
    expect(h.channel[0]).toEqual({
      content: "Watch failing ended: code failed on one: ExecutionFailure: Error: missing secret",
      meta: { watch_id: "failing", event: "lark.im.message.receive_v1" },
    });

    await h.call("watch", {
      event: "lark.im.message.receive_v1",
      id: "stopped",
      code: "return null",
    });
    await h.call("unwatch", { id: "stopped" });
    expect(h.channel).toHaveLength(1);
  } finally {
    await h.close();
  }
});

test("a watch reusing an open id fails and leaves the first watch open", async () => {
  const h = await connect({ harness: "claude" });
  try {
    const watch = { event: "lark.im.message.receive_v1", id: "taken", code: "return null" };
    await h.call("watch", watch);
    const [open] = (await h.call("watches")).watches;
    expect(Object.keys(open).sort()).toEqual([
      "arguments",
      "event",
      "expiresAt",
      "id",
      "startedAt",
    ]);
    const second = await h.callRaw("watch", watch);
    expect(second).toEqual({
      isError: true,
      text: "id is already open in this or another session; choose another id",
    });
    expect((await h.call("watches")).watches.map((open: { id: string }) => open.id)).toEqual([
      "taken",
    ]);
  } finally {
    await h.close();
  }
});
