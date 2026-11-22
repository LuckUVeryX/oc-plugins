import { Schema } from "effect";
import type { Integration } from "../core/integration.ts";
import { commonInstructions, integrationInstructions, type Tools } from "../core/harness.ts";

const instructions = (integrations: ReadonlyArray<Integration>) =>
  [
    "Call list_events to see what this server can watch, with each event's arguments and payload. Arm one with the watch tool. The watch runs filter, prepare, and transform in turn, and transform's returned string reaches this session.",
    "A watch is live. It sees events from the moment it arms until it expires or you stop it, and nothing older is replayed. Choose a watchId. list_watches shows the arguments of every open watch, and stop_watch ends one early.",
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
