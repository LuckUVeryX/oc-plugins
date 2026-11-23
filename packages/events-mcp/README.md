# events-mcp

An agent session only acts when you prompt it. events-mcp lets a session in OpenCode, Claude Code, or Codex wait for something in Lark or GitLab and wake up when it happens.

You tell the agent what to wait for. The agent arms a watch, and the next turn starts when a matching event arrives. Nothing polls, and no tokens are spent while it waits.

## What you can do with it

- **Babysit a pipeline.** The session wakes when a pipeline fails, reads the failed jobs, and fixes the code or retries the job.
- **Answer review comments.** The session wakes on each new comment on your merge request and replies or pushes a fix.
- **Reply to Lark messages that need you.** A small decision model reads the thread and wakes the session only when a message asks something of you.
- **Follow a merge request to the finish.** The session wakes when approvals, reviewers, or merge status change.

## How it works

```text
Lark WebSocket, GitLab Action Cable
  hub: one per user, shares connections and drops duplicates
    OpenCode:           an automation filters each event and wakes the session
    Claude Code, Codex: the hub runs the watch's code, which filters and wakes
```

The filter is a short piece of JavaScript the agent writes. On each event it can:

- read more from Lark or GitLab, such as the failed jobs or the rest of a thread,
- keep a little JSON state between events, such as the last comment it saw,
- ask a decision model a yes-or-no question when plain code cannot tell whether the event matters.

The server only reads. The agent replies with its own tools, such as `lark-cli` or `glab`.

## Events

22 events, listed in full in the [reference](docs/reference.md#events).

- **Lark, 14 events.** Messages to the bot, reactions, read receipts, document comments, task changes, bot menu clicks, and chat settings and membership.
- **GitLab, 8 events.** Pipeline and job status, merge status, reviewers, approvals, comments on a merge request or issue, and any merge request you author, review, or are assigned. A GitLab watch takes a link or a shorthand such as `group/project!123`.

## Setup

You need Git, Bun 1.4.4, and one of OpenCode 2.4.0, Claude Code 2.1.330, or Codex 0.171.0. For each source you use:

- **Lark.** An app in WebSocket event mode, with `LARK_APP_ID` and `LARK_APP_SECRET`.
- **GitLab.** `GITLAB_TOKEN` with `read_api` and `read_repository`, or `glab` logged in. Set `GITLAB_HOST` for self-managed GitLab.

Install:

```sh
git clone https://github.com/LuckUVeryX/oc-plugins.git "$HOME/.local/share/events-mcp"
cd "$HOME/.local/share/events-mcp"
bun install --frozen-lockfile
```

Then add the server to your client. Export the credentials in the environment that starts the client, and keep them out of config files.

### OpenCode

Merge this into `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "experimental": { "mcp_events": true, "location_idle_timeout": false },
  "mcp": {
    "servers": {
      "events": {
        "type": "local",
        "command": [
          "bun",
          "{env:HOME}/.local/share/events-mcp/packages/events-mcp/src/main.ts",
          "opencode",
        ],
        "cwd": "{env:HOME}/.local/share/events-mcp",
        "protocol": "2026-07-28",
        "environment": {
          "LARK_APP_ID": "{env:LARK_APP_ID}",
          "LARK_APP_SECRET": "{env:LARK_APP_SECRET}",
          "GITLAB_TOKEN": "{env:GITLAB_TOKEN}",
        },
      },
    },
  },
}
```

`location_idle_timeout: false` keeps OpenCode from ending long watches when a location goes idle.

### Claude Code

```sh
claude mcp add --scope user events-mcp -- \
  bun "$HOME/.local/share/events-mcp/packages/events-mcp/src/main.ts" claude

MCP_PROTOCOL_NEGOTIATION=legacy claude --dangerously-load-development-channels server:events-mcp
```

Wakes arrive through MCP channels, a research preview. They need claude.ai or Console API key auth and do not reach `claude -p`.

### Codex

Add this to `~/.codex/config.toml`, with your absolute path:

```toml
[mcp_servers.events]
command = "bun"
args = ["/home/you/.local/share/events-mcp/packages/events-mcp/src/main.ts", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 120
env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST"]
```

Run the Codex TUI without `--no-daemon`, using the default `~/.codex` home.

### Try it

Restart your sessions, then ask the agent something like "wake me when the pipeline on `group/project!123` fails". The agent lists the events, writes the watch, and arms it.

## Before you rely on it

- **Watches are live and in memory.** Nothing is replayed. Events that arrive while no watch is open are lost, and a restart of the hub or the session ends its watches.
- **A watch lasts an hour** in Claude Code and Codex unless the agent asks for longer. In OpenCode it lasts until its automation expires or is deleted.
- **Every wake costs a turn.** Filter in the watch code so only events that matter wake the session.
- **One Lark consumer per app.** A watch fails if `lark-cli event consume` or another copy of this server holds the app's connection.
- **Watch code reads whatever your credentials allow.** Grant only the scopes you need.
- **GitLab subscriptions are marked Experiment** and can change on a GitLab upgrade.

After you change credentials or update, end every session so the next one starts a fresh hub:

```sh
git pull --ff-only
bun install --frozen-lockfile
```

The [reference](docs/reference.md) covers client caveats, settings, the hub, the watch workflow and recipes for agents, and troubleshooting.
