import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { IntegrationName, type Integration } from "../../src/integration/contract.ts";
import { connect } from "../harness.ts";

const blocked: Integration = {
  name: Schema.decodeUnknownSync(IntegrationName)("blocked"),
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
    const h = await connect({ harness: "claude", integrations: [blocked] });
    try {
      const armed = await h.call("watch", {
        event: "blocked.ping",
        watchId: "expiring",
        script: "return null",
        expiresInSeconds: 1,
      });
      expect(armed).toEqual({ watchId: "expiring", ended: "Expired" });
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
      watchId: "failing",
      script: "throw new Error('missing secret')",
    });
    h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(1));
    expect(h.channel[0]).toEqual({
      content: "Watch failing ended: script failed on one: ExecutionFailure: Error: missing secret",
      meta: { watch_id: "failing", event: "lark.im.message.receive_v1" },
    });

    await h.call("watch", {
      event: "lark.im.message.receive_v1",
      watchId: "stopped",
      script: "return null",
    });
    await h.call("unwatch", { watchId: "stopped" });
    expect(h.channel).toHaveLength(1);
  } finally {
    await h.close();
  }
});

test("a watch with an open watchId fails and leaves the first watch open", async () => {
  const h = await connect({ harness: "claude" });
  try {
    const watch = { event: "lark.im.message.receive_v1", watchId: "taken", script: "return null" };
    await h.call("watch", watch);
    const second = await h.callRaw("watch", watch);
    expect(second).toEqual({
      isError: true,
      text: "watchId is already active; choose another ID or stop the watch",
    });
    expect(
      (await h.call("watches")).watches.map((open: { watchId: string }) => open.watchId),
    ).toEqual(["taken"]);
  } finally {
    await h.close();
  }
});
