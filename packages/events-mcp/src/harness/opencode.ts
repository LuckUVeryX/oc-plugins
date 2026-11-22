import type { Integration } from "../core/integration.ts";
import { commonInstructions, integrationInstructions, type Events } from "../core/harness.ts";

const instructions = (integrations: ReadonlyArray<Integration>) =>
  [
    "Call watch_mcp_event with only the configured server name to list its events and arguments. Arm one with an event name and its arguments. The watch runs filter, prepare, and transform in turn, and transform's returned string wakes this session.",
    "A watch is live. It sees events from the moment it arms until it expires or you stop it, and nothing older is replayed. Choose a watchId. list_watches shows the arguments of every open watch, and stop_watch ends one early.",
    ...integrationInstructions(integrations),
    ...commonInstructions,
  ].join("\n\n");

export const harness: Events = {
  _tag: "Events",
  capabilities: { extensions: { "io.modelcontextprotocol/events": {} } },
  instructions,
};
