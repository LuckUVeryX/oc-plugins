import { Schema } from "effect";
import { instructions, listEventsInstructions, type Tools } from "./harness.ts";

export const harness: Tools = {
  _tag: "Tools",
  capabilities: { experimental: { "claude/channel": {} } },
  instructions: instructions({
    arm: listEventsInstructions,
    wakes: [
      "Wakes arrive as a <channel> message tagged with watch_id and event. They are one-way. Act on them, and do not reply.",
    ],
  }),
  watchParams: Schema.Struct({}),
  wake: (send, { watchId, event, text }) =>
    send({
      method: "notifications/claude/channel",
      params: { content: text, meta: { watch_id: watchId, event } },
    }),
};
