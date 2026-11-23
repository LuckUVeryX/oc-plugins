import { Schema } from "effect";
import type { Integration } from "../core/integration.ts";
import {
  commonInstructions,
  integrationInstructions,
  listEventsInstructions,
  liveWatchInstructions,
  type Tools,
} from "../core/harness.ts";

const instructions = (integrations: ReadonlyArray<Integration>) =>
  [
    listEventsInstructions,
    liveWatchInstructions,
    ...integrationInstructions(integrations),
    ...commonInstructions,
    "Wakes arrive as a <channel> message tagged with watch_id and event. They are one-way. Act on them, and do not reply.",
  ].join("\n\n");

export const harness: Tools = {
  _tag: "Tools",
  capabilities: { experimental: { "claude/channel": {} } },
  instructions,
  watchParams: Schema.Struct({}),
  wake: (send, { watchId, event, text }) =>
    send({
      method: "notifications/claude/channel",
      params: { content: text, meta: { watch_id: watchId, event } },
    }),
};
