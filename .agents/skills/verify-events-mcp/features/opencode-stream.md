# OpenCode events stream

An OpenCode session lists the watchable events, arms one through the `events` MCP server, and receives each matching event as a stream notification that wakes the agent.

## Sub-features

- `opencode-list` answers `events/list` with 22 events, 14 Lark and 8 GitLab.
- `opencode-arm` opens `events/stream` and reports the watch live with `notifications/events/active`.
- `opencode-wake` delivers a matching GitLab pipeline change as `notifications/events/event` with the script's text.
- `opencode-terminate` ends the watch when the stream request ends, with no replay.

## How to get to it (user POV)

- Configure OpenCode with the `events` local server, harness `opencode`, protocol `2026-07-28`, and `experimental.mcp_events`.
- Ask the agent to call `watch_mcp_event` with the server name to list events, then again with an event and its arguments.
- Change a watched resource. OpenCode receives `notifications/events/event` on the same connection and starts a turn.

## Driving it with mcp-client.mjs

Preconditions:

- `bun scripts/target.ts doctor` reports zero failures.
- The run's hub directory exists; `run.sh init` created it.
- No OpenCode TUI is launched. The client speaks the same MCP connection with the `opencode` harness.

- **List the catalogue.** Run `bun scripts/mcp-client.mjs list`. It prints `protocol: 2026-07-28`, `events: 22`, `lark: 14`, `gitlab: 8`, and the tools `lark_get`, `gitlab_get`, `watches`, `unwatch`. It writes `opencode-list.json`.
- **Arm, trigger, and wait.** Run `bun scripts/mcp-client.mjs watch --id stream-verify --timeout 600 --trigger`. It arms the watch on `gitlab.ciPipelineStatusesUpdated` for `$VERIFY_GITLAB_PROJECT`, waits for `active`, creates one pipeline, and waits for the first event. For a pipeline triggered separately, omit `--trigger` and run `bun scripts/target.ts trigger` after the armed record appears.
- **Armed output.** The command prints `{"armed": {"id": "stream-verify", "subscriptionId": <n>, ...}}` and writes `opencode-armed.json`. The subscription id is the `events/stream` request id.
- **Wake output.** It prints `receivedAt` and `event.data` of the form `pipeline gid://gitlab/Ci::Pipeline/<id> status SUCCESS` and writes `opencode-wake.json`. The exit code is 0 only when the event names the pipeline it triggered.
- **Proof.** `opencode-wake.json` holds the subscription id, the `active` and `event` notification times, and the event id. With `--trigger`, the client writes the pipeline to `opencode-trigger.json`; compare `event.data` with its `id`. For a pipeline triggered separately, run `bun scripts/target.ts trigger | tee "$VERIFY_EVIDENCE/trigger.json"` and compare with that `id`.
- **Cleanup.** Run `scripts/run.sh cleanup "$VERIFY_RUN_DIR"`. The hub exits on its own when the client disconnects; cleanup copies its log into evidence.

## Gotchas

- Do not launch the operator's OpenCode. Its server, storage, and provider config are live state, and a second copy can corrupt them. This recipe drives the protocol below the TUI deliberately; record that the `watch_mcp_event` wrapper itself was not exercised.
- OpenCode pins `2026-07-28` in its server config. The client negotiates the revision automatically and can fall back to a legacy one, so check `protocol` in the `list` output. Any other value means the drive did not use the revision OpenCode requires.
- OpenCode cancels a feed after 60 seconds without a notification, so the server sends `notifications/events/heartbeat` every 20 seconds. A long watch with no heartbeats means the connection is gone.
- `events/stream` takes a `target` in its arguments, as the `watch` tool does, but its result is `{}`. Call `watches` to see the resolved `params`.
- Events are live only. `cursor` must be absent or `null`, and nothing is replayed after a reconnect.
- `events/stream` stays open for the life of the watch. Aborting the request ends the watch without a `terminated` notification.
- The stream id is `_meta["io.modelcontextprotocol/subscriptionId"]`. Match events to the watch through it when more than one stream is open.
- A watch ends when its connection ends. OpenCode's idle location timeout unloads the location and its watches; set `location_idle_timeout` to `false` for long watches.
