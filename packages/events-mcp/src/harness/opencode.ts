import { instructions, type Events } from "./harness.ts";

export const harness: Events = {
  _tag: "Events",
  capabilities: { extensions: { "io.modelcontextprotocol/events": {} } },
  instructions: instructions({
    arm: "Call watch_mcp_event with only the configured server name to list its events and arguments. Arm one with an event name and its arguments. The watch runs its script on each event, and whatever the script returns, unless null, wakes this session.",
  }),
};
