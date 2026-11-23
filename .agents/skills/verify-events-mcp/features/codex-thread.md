# Codex thread wake

An agent in a Codex thread arms a watch with that thread's id, a pipeline changes, and the wake arrives as a new user turn in the same thread.

## Sub-features

- `codex-thread-id` reads the thread id from `$CODEX_THREAD_ID` in the agent's shell and passes it to `watch`.
- `codex-arm` arms a pipeline watch from the `events` server in `config.toml`.
- `codex-wake` delivers a status change as a new user turn in the watched thread.
- `codex-reject` refuses a missing or unknown thread id instead of waking another session.

## How to get to it (user POV)

- Run the Codex TUI normally, with its app-server daemon. `--no-daemon` has no control socket and cannot receive wakes.
- Put the `events` server in the default `~/.codex/config.toml` under the session's home, passing `codex` as the harness and exporting every credential and `EVENTS_MCP_HUB_DIR` for `env_vars` to forward.
- Ask the agent to print `$CODEX_THREAD_ID` and arm a watch with it.
- Change a pipeline in the watched project. The wake appears as a new `›` user turn in the thread.

## Driving it with tmux + Codex

Preconditions:

- `bun scripts/target.ts doctor --codex` reports zero failures.
- Codex uses its default home, `$VERIFY_HOME/.codex`. That directory holds `auth.json` and the generated `config.toml`; `CODEX_HOME` is not set.
- Codex auth reaches that home only by copy. `init` copies `~/.codex/auth.json` to `$VERIFY_CODEX_HOME/auth.json`; nothing writes to `~/.codex`. If the copy is missing, doctor fails and names the one fix: `cp ~/.codex/auth.json "$VERIFY_CODEX_HOME/auth.json" && chmod 600 "$VERIFY_CODEX_HOME/auth.json"`. Copy only `auth.json`.
- `config.toml` has `[mcp_servers.events]` with `args = ["<repo>/packages/events-mcp/src/main.ts", "codex"]` and `env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST", "EVENTS_MCP_HUB_DIR"]`. It holds no secret.
- `$VERIFY_RUN_DIR/bin/codex` exports those five names before it starts Codex; the pane shows only the launcher path.
- The tmux socket and session come from the run.

- **Launch.** Start the TUI. Run `scripts/tui.sh codex`. It starts `$VERIFY_RUN_DIR/bin/codex`, which runs `codex --no-alt-screen -C "$VERIFY_WORK" -a never -s danger-full-access` with the isolated `HOME` and the exported names, and returns at the composer, `Ask Codex to do anything`. A startup warning about bubblewrap is expected in a container; Codex uses its bundled copy.
- **Arm.** Send the watch prompt, replacing `<project path>` with `$VERIFY_GITLAB_PROJECT`:

  ```sh
  scripts/tui.sh prompt <<'EOF'
  Run the shell command: printf %s "$CODEX_THREAD_ID". Then call the events list_events tool, and call its watch tool with watchId codex-verify, event gitlab.ciPipelineStatusesUpdated, params {"projectFullPath": "<project path>"}, filter {"status": ["SUCCESS", "FAILED", "CANCELED"]}, prepare `return input.event.data`, transform `return input.prepared.id`, expiresInSeconds 600, and threadId set to the exact value you printed. Then call watches and reply with one line: ARMED-OK <threadId> <expiresAt>. Do nothing else.
  EOF
  ```

  Wait for `ARMED-OK [0-9a-f]{8}-`. Match the thread UUID, not the words: the prompt text sits in the pane too and contains `ARMED-OK <threadId>`. The pane shows a successful `events.watch` result with `expiresAt` and a line such as `• ARMED-OK <uuid> 2026-11-22T16:04:51.896Z`.

- **Arm proof.** Run `scripts/tui.sh capture "$VERIFY_EVIDENCE/pane-armed.txt"`. It contains the thread id and the successful `watch` result.
- **Trigger.** Run `bun scripts/target.ts trigger | tee "$VERIFY_EVIDENCE/trigger.json"` and note the `id`.
- **Wake.** Run `scripts/tui.sh expect '<pipeline id>' 600`. The pane gains a new `› gid://gitlab/Ci::Pipeline/<pipeline id>` user turn and the agent replies, for example `The codex-verify watch matched pipeline <pipeline id> in <project>`.
- **Wake proof.** Capture the pane with `scripts/tui.sh capture "$VERIFY_EVIDENCE/pane-final.txt"`. Copy the newest rollout that names the pipeline:

  ```sh
  cp "$(grep -rl '<pipeline id>' "$VERIFY_CODEX_HOME/sessions" | sort | tail -1)" "$VERIFY_EVIDENCE/codex-session.jsonl"
  ```

- **Reject.** Arm a second watch with a thread id Codex has never seen. Run it through `scripts/tui.sh prompt`:

  ```text
  Call the events watch tool with watchId codex-reject, event gitlab.ciPipelineStatusesUpdated, params {"projectFullPath": "<project path>"}, prepare `return null`, transform `return "x"`, expiresInSeconds 60, and threadId 00000000-0000-4000-8000-000000000000. Reply with REJECT followed by the exact error text. Then call watches and reply LISTED <count>.
  ```

  The watch fails with `Codex does not know thread "<id>"`, and `watches` lists only the watches armed before it. `prepare` is required; a prompt without it fails schema validation before the thread check runs.

- **Cleanup.** Run `scripts/run.sh cleanup "$VERIFY_RUN_DIR"`. It stops the session on the run's own socket, then that socket's server, the daemon recorded under the isolated home, and the run's hub. It keeps the evidence and removes the rest of the run directory, `auth.json` copy included.

## Gotchas

- Codex gives an MCP server only a fixed environment allowlist and does not pass the shell. `env_vars` forwards each named variable from the environment that started Codex, so export the credentials there and keep them out of the file. A name left out of `env_vars` is absent in the hub; a missing `EVENTS_MCP_HUB_DIR` sends the run to the machine-wide hub.
- Only the default Codex home is supported. Codex uses `~/.codex` and the hub reads the control socket there. A different Codex home fails with `Cannot reach the Codex app-server control socket ... ENOENT`.
- A run with no `$VERIFY_CODEX_HOME/auth.json` opens Codex's sign-in screen and never reaches the composer. `doctor --codex` catches it before launch.
- `--no-daemon` starts no control socket. A watch then fails and the thread cannot be woken.
- `threadId` is wrong when a watch fails with `Codex does not know thread`. Read it inside the thread with `printf %s "$CODEX_THREAD_ID"`.
- After changing the server or a credential, end every session, Codex threads included. A connected Codex keeps the old hub alive.
- The Codex TUI leaves an app-server daemon running after it exits. Cleanup stops it by the pid in the isolated home; killing the TUI alone is not enough.
- If shell commands fail with a sandbox error, use `-s danger-full-access`; `-a never` keeps approvals from blocking the prompt. Do not weaken the sandbox beyond what the container needs.
- `codex mcp list` shows an entry as enabled even when its command does not exist. Only a tool call proves the server connected.
