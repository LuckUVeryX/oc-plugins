# events-mcp verification map

This directory is the maintained source for verifying the user-facing behavior of events-mcp. Read the index before driving the app, then use the matching feature file as the recipe.

## Baseline preconditions

- Launch from this checkout with `eval "$(scripts/run.sh init)"`; every run owns `/tmp/verify-events-mcp/<run-id>`.
- Set `VERIFY_GITLAB_HOST`, `VERIFY_GITLAB_PROJECT`, and `VERIFY_GITLAB_REF` to a real target. Provide `GITLAB_TOKEN` or a `glab` login for that host.
- For Lark features, set `VERIFY_LARK_APP_ID`, `VERIFY_LARK_APP_SECRET`, and `VERIFY_LARK_CHAT_ID` (a p2p chat with the app's bot). Stop any other consumer of the app's events first.
- Put the target's harness on `PATH` and run `bun scripts/target.ts doctor`. Require zero failures before driving anything; add `--codex` on any route that drives Codex, which makes a missing Codex auth a failure.
- Never drive the operator's OpenCode, the machine-wide hub, `~/.codex`, or the real Claude home. Every recipe here uses the run's isolated hub, home, and tmux socket.
- In a new shell, reload the run with `eval "$(scripts/run.sh use <run-dir>)"`.

## Driving conventions

- Start every recipe from the baseline state unless its preconditions say otherwise.
- Treat every command and prompt as literal. Keep quotes, backticks, and flags unchanged.
- Run against the run's socket only: `scripts/tui.sh` reads `VERIFY_TMUX_SOCKET` and `VERIFY_TMUX_SESSION`, both `verify-<run-id>`, and addresses the session by exact name.
- Trigger events with `bun scripts/target.ts trigger`. It creates one pipeline on the configured ref; that is the only write a run makes.
- Read private data only through the read tools. Do not put target names, hostnames, chat ids, or user details into committed files.
- Copy transcripts and panes into `$VERIFY_EVIDENCE` before cleanup. Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- A wake proof is the wake text in the harness transcript plus the matching pipeline from `trigger.json`, not a tool call that returned success.
- Claude Code proof includes the pane line beginning `← events-mcp:` and the same pipeline id in the session JSONL.
- Codex proof includes a new `›` user turn with the wake text and the same id in its rollout JSONL.
- OpenCode proof includes the `notifications/events/event` record with its subscription id and the same pipeline id.
- Mutation proof reads the pipeline back from the GitLab API and compares status and ref.
- Record the feature id and the harness on every artifact.
- Report an unreachable path with the attempted command and the unmet precondition. Do not report a skipped entry point as verified through a different path.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with <harness>` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands, and observable proof.

## Features

- [Claude Code channel wake](./claude-channel.md) covers arming a GitLab pipeline watch from a target and receiving the wake as a channel message.
- [Codex thread wake](./codex-thread.md) covers arming a watch with a thread id and receiving the wake as a new turn in that thread.
- [OpenCode events stream](./opencode-stream.md) covers the `opencode` harness over stdio: event list, stream arm, and wake notification.
- [Shared hub](./shared-hub.md) covers several sessions sharing one hub process, one socket, and one upstream connection.
- [Read tools](./read-tools.md) covers `gitlab_get` and `lark_get` from a prepare script or the agent.
