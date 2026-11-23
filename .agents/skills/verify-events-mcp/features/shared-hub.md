# Shared hub

Every harness session for this user connects to one hub, so one Lark WebSocket and one GitLab connection serve them all. Sessions that stay connected share upstream subscriptions and the dedup cache.

## Sub-features

- `hub-single` starts one hub per hub directory and binds one socket.
- `hub-share` serves several simultaneous sessions from that one process.
- `hub-forward` serves every session from one list of watches, so `watches` in any session lists the watches of all of them.
- `hub-exit` ends the hub after the last session closes.
- `hub-log` records the listen line with the hub pid, which is what cleanup and proof read.

## How to get to it (user POV)

- Any harness starts the server normally; the server connects to the hub and starts one when none is running.
- A second OpenCode, Claude Code, or Codex session on the same machine connects to the same hub without extra configuration.
- Nothing in a session names the hub; the shared socket is `$EVENTS_MCP_HUB_DIR/run/hub.sock` for a run, or the user runtime directory in normal use.

## Driving it with run.sh + mcp-client.mjs

Preconditions:

- `bun scripts/target.ts doctor` reports zero failures.
- The run is fresh (`run.sh init`), so no hub from an earlier command is serving.
- The hub log does not exist yet, or you accept `reusedHub` in the output.

- **Open two sessions.** Run `bun scripts/mcp-client.mjs hub --sessions 2`. It opens two stdio connections with the `opencode` harness against the run's hub, lists events in each, and writes `shared-hub.json`.
- **One process.** The output has `"hubPids": ["<pid>"]`, one element. Two pids, or a pid from another run's log, means sessions did not share a hub.
- **Two live sessions.** The output has `"concurrent": 2` and two connection ids in `openConnections`. `opened` is larger because the client opens a short negotiation connection first; only the open set counts.
- **Same catalogue.** Each entry in `sessions` reads `{"session": <n>, "events": 22, "watches": 0}`. `watches` counts the whole hub, so it is 0 only while no session has a watch open.
- **Evidence.** `shared-hub.json` holds the hub dir, `sessions`, the pids, `reusedHub`, `opened`, `concurrent`, the open connections, and the log path. Copy `hub.log` through cleanup, which keeps the `listening at ... as pid <pid>` line and each `connection opened`.
- **Exit.** Run `scripts/run.sh cleanup "$VERIFY_RUN_DIR"`. With no connections left, the hub logs `no connections, exiting` and stops.

## Gotchas

- A fresh run matters. Each sequential command starts and stops its own hub, so the log of a reused run accumulates several `listening` lines and pids. `shared-hub.json` reports `reusedHub: true` when the sessions joined an existing hub instead of starting one.
- Two proxies can spawn a hub at the same moment. One binds, the other logs `another hub is listening` and exits. The listen lines with `as pid` must still name one pid; a client connection that lands on the loser is queued by the winner.
- This recipe proves session sharing, not the upstream half. Proving one Lark WebSocket requires `VERIFY_LARK_*` credentials, a p2p chat, and a second consumer attempt, which must fail with a message that begins `Another connection for this Lark app is online (<n>).`
- The hub keeps the environment of the session that started it. After changing a credential, end every session, Codex threads included, so the next one starts a new hub. A connected Codex keeps the old hub alive.
- Watches and dedup live in memory. A hub restart drops them; there is no re-arm.
- Cleanup reads the pid from this run's log and checks `EVENTS_MCP_HUB_DIR` in the process environment before signaling it. Never kill the machine-wide hub by name.
