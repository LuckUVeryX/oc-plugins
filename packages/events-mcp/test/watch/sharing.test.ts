import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

test("an event published as the second watch becomes active reaches both watches", async () => {
  let publish: (() => void) | undefined;
  const h = await connect({
    transport: (transport) => {
      const send = transport.send.bind(transport);
      let active = 0;
      transport.send = (message, options) => {
        if ("method" in message && message.method === "notifications/events/active") {
          active += 1;
          if (active === 2) publish?.();
        }
        return send(message, options);
      };
    },
  });
  publish = () => h.publish({ eventId: "shared", payload: { message: { chat_id: "oc_shared" } } });
  try {
    const ofType = (type: string) =>
      h.notifications.filter((notification) => notification.type === type);
    h.stream({
      id: "first",
      script: "return input.event.data",
    });
    await vi.waitFor(() => expect(ofType("active")).toHaveLength(1));
    h.stream({
      id: "second",
      script: "return input.event.data",
    });
    await vi.waitFor(() => expect(ofType("event")).toHaveLength(2));
    expect(new Set(ofType("event").map((notification) => notification.id))).toEqual(
      new Set(ofType("active").map((notification) => notification.id)),
    );
  } finally {
    await h.close();
  }
});
