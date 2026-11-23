import { expect, test, vi } from "vitest";
import { connect } from "../harness.ts";

test("a watch armed with the watch tool wakes the session on the channel", async () => {
  const h = await connect({ harness: "claude" });
  try {
    const listed = await h.call("list_events");
    expect(listed.events.map((event: { name: string }) => event.name)).toContain(
      "lark.im.message.receive_v1",
    );

    const armed = await h.call("watch", {
      event: "lark.im.message.receive_v1",
      watchId: "pipeline",
      params: { group: "mine" },
      script: "return `wake: ${input.event.data.message.chat_id}`",
    });
    expect(armed.watchId).toBe("pipeline");

    h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(1));
    expect(h.channel[0]).toEqual({
      content: "wake: oc_one",
      meta: { watch_id: "pipeline", event: "lark.im.message.receive_v1" },
    });
  } finally {
    await h.close();
  }
});
