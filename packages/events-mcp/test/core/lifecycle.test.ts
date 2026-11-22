import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { IntegrationName, type Integration } from "../../src/core/integration.ts";
import { connect } from "../harness.ts";

const blocked: Integration = {
  name: Schema.decodeUnknownSync(IntegrationName)("blocked"),
  tools: {},
  events: [
    {
      name: "ping",
      description: "A ping.",
      params: { type: "object", properties: {}, additionalProperties: false },
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
        prepare: "return null",
        transform: "return 'unused'",
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

test("a failed watch wakes the session, and stop_watch does not", async () => {
  const h = await connect({ harness: "claude" });
  try {
    await h.call("watch", {
      event: "lark.im.message.receive_v1",
      watchId: "failing",
      prepare: "throw new Error('missing secret')",
      transform: "return 'unused'",
    });
    h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(1));
    expect(h.channel[0]).toEqual({
      content:
        "Watch failing ended: prepare failed on one: ExecutionFailure: Error: missing secret",
      meta: { watch_id: "failing", event: "lark.im.message.receive_v1" },
    });

    await h.call("watch", {
      event: "lark.im.message.receive_v1",
      watchId: "stopped",
      prepare: "return null",
      transform: "return 'unused'",
    });
    await h.call("stop_watch", { watchId: "stopped" });
    expect(h.channel).toHaveLength(1);
  } finally {
    await h.close();
  }
});
