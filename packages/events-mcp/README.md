# events-mcp

An agent session only acts when you prompt it. events-mcp lets a session in OpenCode, Claude Code, or Codex wait for something in Lark, GitLab, or Codemagic and wake up when it happens.

You tell the agent what to wait for. The agent arms a watch, and the next turn starts when a matching event arrives. Nothing polls, and no tokens are spent while it waits.

## What you can do with it

- **Babysit a pipeline.** The session wakes when a pipeline fails, reads the failed jobs, and fixes the code or retries the job.
- **Babysit a Codemagic build.** The session wakes on the statuses you care about, such as `finished` or `failed`.
- **Answer review comments.** The session wakes on each new comment on your merge request and replies or pushes a fix.
- **Reply to Lark messages that need you.** A small decision model reads the thread and wakes the session only when a message asks something of you.
- **Follow a merge request to the finish.** The session wakes when approvals, reviewers, or merge status change.

## How it works

```text
Lark WebSocket, GitLab Action Cable, Codemagic Socket.IO
  hub: one per user, shares connections and drops duplicates
    OpenCode:           an automation filters each event and wakes the session
    Claude Code, Codex: the hub runs the watch's code, which filters and wakes
```

The filter is a short piece of JavaScript the agent writes. On each event it can:

- read more from Lark, GitLab, or Codemagic, such as the failed jobs or the rest of a thread,
- keep a little JSON state between events, such as the last comment it saw,
- ask a decision model a yes-or-no question when plain code cannot tell whether the event matters.

The server reads, and can start and cancel Codemagic builds and create public artifact links. The agent replies with its own tools, such as `lark-cli` or `glab`.

## Events

23 events, listed in full in the [reference](docs/reference.md#events).

- **Lark, 14 events.** Messages to the bot, reactions, read receipts, document comments, task changes, bot menu clicks, and chat settings and membership.
- **GitLab, 8 events.** Pipeline and job status, merge status, reviewers, approvals, comments on a merge request or issue, and any merge request you author, review, or are assigned. A GitLab watch takes a link or a shorthand such as `group/project!123`.
- **Codemagic, 1 event.** Build status, for one build or every build in an app.

## Setup

You need Bun 1.4.4, and one of OpenCode 2.4.1, Claude Code 2.1.330, or Codex 0.171.0. For each source you use:

- **Lark.** An app in WebSocket event mode, with `LARK_APP_ID` and `LARK_APP_SECRET`.
- **GitLab.** `GITLAB_TOKEN` with `read_api` and `read_repository`, or `glab` logged in. Set `GITLAB_HOST` for self-managed GitLab.
- **Codemagic.** `CODEMAGIC_TOKEN`, a Codemagic API token.

The server is published on npm. `bunx` downloads it into Bun's cache and runs the `events-mcp` bin, so you do not need to clone this repository. The package pins its direct dependencies. `bunx` does not use the repository's `bun.lock`, so indirect dependencies can change without a release.

Keep the credentials in the environment that starts the client, out of config files.

### OpenCode

Merge this into `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "experimental": { "mcp_events": true, "location_idle_timeout": false },
  "mcp": {
    "servers": {
      "events": {
        "type": "local",
        "command": ["bunx", "@luckuveryx/events-mcp@latest", "opencode"],
        "protocol": "2026-07-28",
        "environment": {
          "LARK_APP_ID": "{env:LARK_APP_ID}",
          "LARK_APP_SECRET": "{env:LARK_APP_SECRET}",
          "GITLAB_TOKEN": "{env:GITLAB_TOKEN}",
          "CODEMAGIC_TOKEN": "{env:CODEMAGIC_TOKEN}",
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
  bunx @luckuveryx/events-mcp@latest claude

MCP_PROTOCOL_NEGOTIATION=legacy claude --dangerously-load-development-channels server:events-mcp
```

Wakes arrive through MCP channels, a research preview. They need claude.ai or Console API key auth and do not reach `claude -p`.

### Codex

Add this to `~/.codex/config.toml`:

```toml
[mcp_servers.events]
command = "bunx"
args = ["@luckuveryx/events-mcp@latest", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 120
env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST", "CODEMAGIC_TOKEN"]
```

Run the Codex TUI without `--no-daemon`, using the default `~/.codex` home.

### Pin a version

Replace `@latest` with an exact version, such as `@0.1.0`, in every client. Clients on one version do not replace each other's hub.

### Try it

Restart your sessions, then ask the agent something like "wake me when the pipeline on `group/project!123` fails". The agent lists the events, writes the watch, and arms it.

## Update

`bunx @luckuveryx/events-mcp@latest` resolves again when a client starts its server process:

- **OpenCode.** Run `opencode service restart`. Its MCP processes belong to the background service, so closing and reopening a window is not enough.
- **Claude Code and Codex.** Reopen the session.

### Versions and the hub

Each MCP server process sends its version to the hub before it speaks MCP. A newer client replaces an older hub when no OpenCode session is connected. The hub replies `replaced`, exits, and the client starts a new hub. Claude Code and Codex watches on the old hub end, so arm them again.

An OpenCode connection keeps the old hub. A newer Claude Code or Codex client joins it and prints a warning naming both versions; run `opencode service restart` to update the hub. An older client works with a newer hub. A client from before 0.1.0 cannot join one and its server process exits, so update every client.

A hub from a version before 0.1.0 does not answer the version line. A new client waits 5 seconds, then exits with `events-mcp: the running hub predates version checks; every OpenCode, Claude Code and Codex session must be closed once.` Close every client once: `opencode service restart` for OpenCode, and reopen Claude Code and Codex. The first npm release meets the hub started from the old checkout, so do this once after upgrading.

## Before you rely on it

- **Watches are live and in memory.** Nothing is replayed. Events that arrive while no watch is open are lost, and a restart of the hub or the session ends its watches.
- **A watch lasts an hour** in Claude Code and Codex unless the agent asks for longer. In OpenCode it lasts until its automation expires or is deleted.
- **Every wake costs a turn.** Filter in the watch code so only events that matter wake the session.
- **One Lark consumer per app.** A watch fails if `lark-cli event consume` or another copy of this server holds the app's connection.
- **Watch code reads whatever your credentials allow, and can start and cancel Codemagic builds and create public artifact links.** Grant only the scopes you need.
- **GitLab subscriptions are marked Experiment** and can change on a GitLab upgrade.
- **Codemagic push is undocumented.** It is the connection Codemagic's web app uses, and it can change without notice.

After you change credentials, run `opencode service restart` and reopen Claude Code and Codex. The hub keeps the environment of the process that started it, and a connected Codex keeps the old hub alive.

The [reference](docs/reference.md) covers client caveats, settings, the hub, the watch workflow and recipes for agents, and troubleshooting.

## Release

1. Update `version` in the root `package.json`, then run:

   ```sh
   bun run sync:metadata
   bun install
   bun run format
   ```

   Commit the change.

2. Tag the commit `v<version>` and push the tag. The publish workflow checks that the tag matches `packages/events-mcp/package.json`, runs `bun run check` and `bun run test:install`, then stages `@luckuveryx/events-mcp@<version>` under the `latest` tag. The package's trusted publisher on npm must name this repository, the `publish-events-mcp.yml` workflow, and the `npm` environment, and allow staged publishing. A new trusted publisher expires unless it stages a release within 2 days, so add it just before you tag.

   For the first release the package does not exist yet. Run `npm stage publish` from `packages/events-mcp` instead of pushing a tag. npm publishes a public `0.0.0-stage` placeholder, and `latest` may point at it until you approve the release. Add the trusted publisher afterwards.

3. Inspect the staged release, then approve it with 2FA:

   ```sh
   npm stage list @luckuveryx/events-mcp
   npm stage download <stage-id>
   npm stage approve <stage-id>
   ```

   You can also approve it in the Staged Packages tab on npmjs.com.

Approving the release makes it `latest`, so clients that use `@latest` get it at their next restart.

Moving `latest` back does not replace a hub that already runs a newer version. Publish a higher version to roll back, rather than moving the tag.
