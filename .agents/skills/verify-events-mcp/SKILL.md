---
name: verify-events-mcp
description: Prove events-mcp wakes a real agent session. Use when changing a harness, the hub, a watch, or an integration, and you need the wake path driven end to end instead of unit tests.
---

# Verify events-mcp

events-mcp is an MCP server that wakes an agent session in OpenCode, Claude Code, or Codex when Lark or GitLab sends an event. The user-facing behavior is a wake: an armed watch, a real event, and a turn in the session that names it. This skill drives that path in the real harnesses, captures evidence, and leaves the machine as it found it.

Read `features/README.md` before driving anything. It owns the feature map; this file owns launch, isolation, evidence, and cleanup.

## What the run needs

- This checkout with `bun install --frozen-lockfile` done at the repo root.
- `bun` 1.4.x, `tmux` 3.4+, and the harness you drive: `claude` 2.1.x, `codex` 0.171.x, or both.
- A GitLab target reachable with a token: `VERIFY_GITLAB_HOST` (bare host, no scheme), `VERIFY_GITLAB_PROJECT` (full path such as `group/project`), `VERIFY_GITLAB_REF` (branch or tag). Export `GITLAB_TOKEN`, or authenticate `glab` for the host; the run resolves the token from either, keeps it in `$RUN_DIR/bin/env` (mode 600), and never prints it.
- For Lark, also set `VERIFY_LARK_APP_ID` (`cli_` plus 16 hex characters), `VERIFY_LARK_APP_SECRET`, and `VERIFY_LARK_CHAT_ID` (a p2p chat with the app's bot). Only one consumer may hold the app's WebSocket, so stop other consumers before a Lark run.
- The real targets are never committed. They live in your shell and in the run directory, which cleanup deletes.
- A run creates one real pipeline on `VERIFY_GITLAB_REF` and nothing else in GitLab. Use a ref where an extra pipeline is acceptable.

`scripts/run.sh init` copies `~/.claude.json`, `~/.claude/.credentials.json`, and `~/.codex/auth.json` into the run's own home. The copies are deleted with the run; the real homes are never written. That copy is how a run gets Codex auth: `$RUN_HOME/.codex/auth.json`. When it is missing, `bun scripts/target.ts doctor --codex` fails and prints the exact `cp ~/.codex/auth.json "$VERIFY_CODEX_HOME/auth.json"` fix. Never write to `~/.codex` and never point `CODEX_HOME` elsewhere. The copied `~/.claude.json` keeps the CLI out of first-run onboarding.

## Launch

Start every run with its own hub, home, tmux socket, and evidence directory:

```sh
export VERIFY_GITLAB_HOST=... VERIFY_GITLAB_PROJECT=... VERIFY_GITLAB_REF=...
eval "$(scripts/run.sh init)"
bun scripts/target.ts doctor
```

`init` prints `export` lines and no secret; it creates `/tmp/verify-events-mcp/<run-id>/{home,hub,work,evidence}` and points `EVENTS_MCP_HUB_DIR` at `/tmp/verify-events-mcp/<run-id>/hub`, so the server never touches the machine-wide hub. The run's tmux socket and session are both `verify-<run-id>`, unique per run, so one run's cleanup cannot reach another run. Paths in this file are relative to this skill directory. In a new shell, reload the run with `eval "$(scripts/run.sh use <run-dir>)"`.

Launch a harness only through the run:

- **Claude Code.** `scripts/tui.sh claude` starts `$VERIFY_RUN_DIR/bin/claude` on the run's tmux socket, then answers the two first-launch dialogs. It returns after `Channels (experimental) messages from server:events-mcp` appears. The launcher sources the run's credentials from `bin/env`, sets `MCP_PROTOCOL_NEGOTIATION=legacy`, and adds `--dangerously-load-development-channels server:events-mcp`; `run.sh init` writes it.
- **Codex.** `scripts/tui.sh codex` starts `$VERIFY_RUN_DIR/bin/codex`, which sets the isolated `HOME`, the run's hub, and the names Codex forwards. Codex uses its default `~/.codex` under that home, where `init` wrote the `events` server, an `env_vars` list, and a copy of `auth.json`. Readiness is the composer prompt. Do not set `CODEX_HOME`: Codex must use the default `~/.codex`, and the server reads the control socket there.
- **OpenCode.** Do not launch the operator's OpenCode. Drive the `opencode` harness over stdio with `scripts/mcp-client.mjs`, which opens the same MCP connection and calls `events/list` and `events/stream`. The TUI's `watch_mcp_event` wrapper is not exercised; everything below it is.

## Doctor

`bun scripts/target.ts doctor` is read-only. It checks the run layout (hub dir inside `/tmp/verify-events-mcp`, isolated home and work dirs, copied auth, tmux socket `verify-<run-id>`), the harness versions, the GitLab token through `GITLAB_TOKEN`, `bin/env`, or `glab`, the project and ref through the API, and the Lark app and chat when those variables are present. It exits 1 on any failure and prints no secret. Pass `--all` to require the Lark checks; pass `--codex` before the Codex feature, which turns a missing Codex auth into a failure with the copy command to run. Run it first, and again whenever a drive fails for an unclear reason.

## Drive

Each feature file has the literal prompt and the expected text. The shape is the same for both TUIs:

1. Launch with the helper and wait for its ready line.
2. Feed the feature's prompt with `scripts/tui.sh prompt <<'EOF' ... EOF`. The helper types the text, then sends Enter twice; the first Enter sometimes lands before the composer settles.
3. Wait for the armed line with `scripts/tui.sh expect '<pattern>' 120`. Match a value only the reply can hold, such as the expiry timestamp or the thread UUID. The prompt text is in the pane and matches a bare `ARMED-OK`.
4. Trigger one real event with `bun scripts/target.ts trigger`, which creates a pipeline on `VERIFY_GITLAB_REF` and prints its id.
5. Wait for the wake with `scripts/tui.sh expect '<pipeline id>' 600`. The wait is proof only when the transcript later names the same id.

`scripts/tui.sh capture <file>` writes the visible pane and 400 lines of scrollback. Use it before and after the trigger.

Every session command runs on the run's own socket and addresses the session by exact name (`=verify-<run-id>`), and every server process gets `EVENTS_MCP_HUB_DIR` from the run. Nothing here may point at a hub outside the run: a wake in the operator's session is a failed run, not a pass. The launchers in `$VERIFY_RUN_DIR/bin` source `bin/env`, which holds the credentials, so no secret reaches the tmux command line or a captured pane.

For Lark, drive a message to the bot in `VERIFY_LARK_CHAT_ID` after the watch is armed; the wake text is the message payload.

## Evidence

Every artifact goes to `$VERIFY_EVIDENCE` (`/tmp/verify-events-mcp/<run-id>/evidence`). A proof captures the action and the result:

- The pane before the trigger (watch armed) and after it (wake), from `scripts/tui.sh capture`.
- The harness transcript: `$VERIFY_HOME/.claude/projects/*/*.jsonl` for Claude, `$VERIFY_CODEX_HOME/sessions/*/*/*/rollout-*.jsonl` for Codex. Copy it into evidence before cleanup.
- `trigger.json` from `target.ts trigger`, and the pipeline read back from the API if the status matters.
- `hub.log`, copied by cleanup, which shows the one hub the sessions shared.
- For the stdio drives, `mcp-client.mjs` writes `opencode-list.json`, `opencode-armed.json`, `opencode-wake.json`, `read-tools.json`, or `shared-hub.json` itself.

A claim needs the wake text in the transcript and the same pipeline id in `trigger.json`. A pending request, a green server log, or a tool that returned without an error is not a wake. Do not tidy a transcript; copy it whole.

## Cleanup

```sh
scripts/run.sh cleanup <run-dir>
```

Cleanup kills this run's tmux session by exact name, then this run's tmux server, whose socket is `verify-<run-id>`, then this run's Codex daemons (pids recorded under the isolated home) and this run's hub (pid from its own log, checked against `EVENTS_MCP_HUB_DIR`). No other run shares the socket, and nothing is killed by process name. After changing the server or a credential, end every session, Codex threads included; a connected Codex keeps the old hub alive. It copies `hub.log` into evidence, deletes `home`, `work`, `hub`, and `bin` (including the credential file), and removes the run marker `.verify-run` and `.target`, which holds the target and never a secret. The run directory is left holding `evidence/` alone; `run.sh status` and `run.sh use` no longer apply to it. Evidence survives; if it does not, the run failed to prove anything. Run cleanup after a failed attempt too, before the next one.

## Helpers

| Helper                   | Invocation                                                       | Purpose                                         |
| ------------------------ | ---------------------------------------------------------------- | ----------------------------------------------- |
| `scripts/run.sh`         | `init`, `use <run-dir>`, `status <run-dir>`, `cleanup <run-dir>` | Run layout, isolated homes, launchers, teardown |
| `scripts/target.ts`      | `doctor [--all] [--codex]`, `trigger`                            | Read-only target checks; the one pipeline write |
| `scripts/tui.sh`         | `claude`, `codex`, `prompt`, `expect`, `key`, `capture`, `stop`  | tmux driver and measured dialog keys            |
| `scripts/mcp-client.mjs` | `list`, `read-tools`, `watch [--trigger]`, `hub [--sessions N]`  | OpenCode surface over stdio                     |
