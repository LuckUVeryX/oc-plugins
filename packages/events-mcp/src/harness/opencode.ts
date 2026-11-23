import type { Integration } from "../core/integration.ts";
import {
  commonInstructions,
  integrationInstructions,
  liveWatchInstructions,
  type Events,
} from "../core/harness.ts";

const instructions = (integrations: ReadonlyArray<Integration>) =>
  [
    "Call watch_mcp_event with only the configured server name to list its events and arguments. Arm one with an event name and its arguments. The watch runs its script on each event, and whatever the script returns, unless null, wakes this session.",
    liveWatchInstructions,
    ...integrationInstructions(integrations),
    ...commonInstructions,
  ].join("\n\n");

export const harness: Events = {
  _tag: "Events",
  capabilities: { extensions: { "io.modelcontextprotocol/events": {} } },
  instructions,
};
