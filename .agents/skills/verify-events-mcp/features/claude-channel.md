# Claude Code channel wake

An agent in Claude Code arms a watch on GitLab, a pipeline changes, and the wake arrives as a channel message tagged with the server name that starts a turn in the session.

## Sub-features

- `claude-arm` lists the 22 events and arms one pipeline watch through the `list_events` and `watch` tools.
- `claude-wake` delivers a pipeline status change as a `notifications/claude/channel` message and starts a turn while the session is idle.
- `claude-stop` ends the watch with `stop_watch`, and `list_watches` no longer lists it.
- `claude-transcript` records the channel text and the turn in the session JSONL, the durable receipt Claude Code gives.

## How to get to it (user POV)

- Start Claude Code with the events-mcp server entry and `MCP_PROTOCOL_NEGOTIATION=legacy`.
- Launch it with `--dangerously-load-development-channels server:<name>`, where `<name>` is the server entry name (`events-mcp` here), and confirm the development-channel warning.
- Ask the agent to list events and arm a watch on a GitLab project.
- Change a pipeline in that project. The wake appears as `← events-mcp: <transform text>` and the agent acts on it in a new turn.

## Driving it with tmux + Claude Code

Preconditions:

- `bun scripts/target.ts doctor` reports zero failures.
- `$VERIFY_HOME/.claude/.credentials.json` exists; `run.sh init` copied it.
- `$VERIFY_WORK/.mcp.json` names the server `events-mcp` with `bun <repo>/packages/events-mcp/src/main.ts claude`.
- The tmux socket and session come from the run. Do not attach to another socket.

- **Launch.** Start the TUI and answer the first-launch dialogs. Run `scripts/tui.sh claude`. It starts `$VERIFY_RUN_DIR/bin/claude`, which holds the run's credentials and flags, so the pane shows only that path. It waits for `Quick safety check` and answers Yes, then waits for `Loading development channels` and confirms option 1. Ready is the banner `Channels (experimental) messages from server:events-mcp`.
- **Arm.** Send the watch prompt, replacing `<project path>` with `$VERIFY_GITLAB_PROJECT`. Run:

  ```sh
  scripts/tui.sh prompt <<'EOF'
  Use the events-mcp list_events tool, then the events-mcp watch tool. Arm watchId claude-verify, event gitlab.ciPipelineStatusesUpdated, params {"projectFullPath": "<project path>"}, filter {"status": ["SUCCESS", "FAILED", "CANCELED"]}, prepare `return input.event.data`, transform `return input.prepared.id`, expiresInSeconds 600. Then call list_watches and reply with one line: ARMED-OK <watchId> <expiresAt>. Do nothing else.
  EOF
  ```

  Wait for `ARMED-OK claude-verify 20`. Match the timestamp, not the words: the prompt text sits in the pane too and contains `ARMED-OK <watchId>`. The pane shows `Called events-mcp 3 times` and a line such as `● ARMED-OK claude-verify 2026-11-22T15:52:42.683Z`.

- **Arm proof.** Capture the pane. Run `scripts/tui.sh capture "$VERIFY_EVIDENCE/pane-armed.txt"`. It contains the `ARMED-OK` line and the expiry.
- **Trigger.** Create one pipeline. Run `bun scripts/target.ts trigger | tee "$VERIFY_EVIDENCE/trigger.json"`. Note the `id`.
- **Wake.** Wait for the pipeline id in the pane. Run `scripts/tui.sh expect '<pipeline id>' 600`. The pane gains `← events-mcp: gid://gitlab/Ci::Pipeline/<pipeline id>` followed by the agent's turn, for example `● Pipeline gid://gitlab/Ci::Pipeline/<pipeline id> in <project> reached SUCCESS, FAILED or CANCELED`.
- **Wake proof.** Capture the pane after the turn, then copy the transcript. Run `scripts/tui.sh capture "$VERIFY_EVIDENCE/pane-final.txt"`, then `cp "$VERIFY_HOME/.claude/projects"/*/*.jsonl "$VERIFY_EVIDENCE/claude-session.jsonl"`. The JSONL contains `<channel>` and the same pipeline id.
- **Stop.** Send `Call the events-mcp stop_watch tool with watchId claude-verify, then call list_watches and reply STOPPED <count>.` Run it through `scripts/tui.sh prompt`. The pane shows `STOPPED 0`.
- **Cleanup.** Run `scripts/run.sh cleanup "$VERIFY_RUN_DIR"`. `hub.log`, both panes, `trigger.json`, and the JSONL remain under `$VERIFY_EVIDENCE`.

## Gotchas

- The trust dialog and the development-channel warning both appear on a fresh home. The first Enter on the trust dialog exits; `tui.sh claude` selects Yes deliberately. Do not hand-type through the dialogs.
- `--dangerously-load-development-channels` is ignored in `claude -p` and in the Agent SDK. A headless run cannot prove a channel wake; the TUI is the surface.
- Without `MCP_PROTOCOL_NEGOTIATION=legacy` the channel registers nothing and the server logs no wake. The banner then reads that channel messages are unavailable.
- Claude sends no acknowledgement for a channel message, and it queues messages while busy. `list_watches` is the only proof that the watch is still live.
- The composer can show a gray suggestion such as `stop the claude-verify watch`. It is not typed input and does not affect the run.
- `transform` must return a string or `null`. Returning a number or object makes the watch stop with `transform failed`.
- The isolated home holds copies of the real `~/.claude.json` and credentials. Cleanup deletes them; never copy the run home into evidence.
