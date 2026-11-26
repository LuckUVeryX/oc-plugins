# events-mcp

events-mcp wakes an OpenCode, Claude Code, or Codex session when something happens in Lark, GitLab, or Codemagic. You tell the agent what to wait for, and it arms a watch. Waiting uses no tokens.

Use it to:

- fix or retry a failed pipeline or Codemagic build
- answer comments on a merge request
- reply to Lark messages that ask you for something
- follow a merge request until it merges

## How it works

```text
Lark WebSocket, GitLab Action Cable, Codemagic Socket.IO
  hub: one per user, shares connections and drops duplicates
    OpenCode:           an automation filters each event and wakes the session
    Claude Code, Codex: the hub runs the watch's code, which filters and wakes
```

The agent writes the filter in JavaScript. It can read more from the source, keep JSON state between events, and ask a decision model a yes-or-no question.

The server only reads Lark and GitLab. It can also start and cancel Codemagic builds and create public artifact links. The agent replies with its own tools, such as `lark-cli` or `glab`.

## Events

The [reference](docs/reference.md#events) lists all 23 events.

- **Lark.** Messages to the bot, reactions, read receipts, document comments, tasks, bot menu clicks, and chat changes.
- **GitLab.** Pipelines, jobs, merge status, reviewers, approvals, and comments.
- **Codemagic.** Build status, for one build or every build in an app.

## Setup

You need Node.js 24 or later, and one of OpenCode 2.4.1, Claude Code 2.1.330, or Codex 0.171.0. For each source you use:

- **Lark.** An app in WebSocket event mode, with `LARK_APP_ID` and `LARK_APP_SECRET`.
- **GitLab.** `GITLAB_TOKEN` with `read_api` and `read_repository`, or `glab` logged in. Set `GITLAB_HOST` for self-managed GitLab.
- **Codemagic.** `CODEMAGIC_TOKEN`, a Codemagic API token.

Put the credentials in the environment that starts the client, not in config files.

### OpenCode

Merge this into `~/.config/opencode/opencode.jsonc`:

```jsonc
{
  "experimental": { "mcp_events": true, "location_idle_timeout": false },
  "mcp": {
    "servers": {
      "events": {
        "type": "local",
        "command": ["npx", "-y", "@luckuveryx/events-mcp@latest", "opencode"],
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
  npx -y @luckuveryx/events-mcp@latest claude

MCP_PROTOCOL_NEGOTIATION=legacy claude --dangerously-load-development-channels server:events-mcp
```

Wakes arrive through MCP channels, a research preview. They need claude.ai or Console API key auth and do not reach `claude -p`.

### Codex

Add this to `~/.codex/config.toml`:

```toml
[mcp_servers.events]
command = "npx"
args = ["-y", "@luckuveryx/events-mcp@latest", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 120
env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST", "CODEMAGIC_TOKEN"]
```

Run the Codex TUI without `--no-daemon`, using the default `~/.codex` home.

## Update

Clients on `@latest` update when their server process restarts. Run `opencode service restart` for OpenCode, and reopen Claude Code and Codex. To pin a version, replace `@latest` with one such as `@0.1.0` in every client.

A newer client replaces an older hub unless an OpenCode session is connected. Claude Code and Codex watches on the old hub end, so arm them again. When OpenCode is connected, the newer client joins the old hub and prints a warning. See [the hub](docs/reference.md#the-hub) for version rules.

The package pins its direct dependencies. Indirect dependencies can change without a release, because `npx` does not use this repository's `bun.lock`.

## Before you rely on it

- **Watches are live and in memory.** Nothing is replayed. Events that arrive while no watch is open are lost, and a restart of the hub or the session ends its watches.
- **A watch lasts an hour** in Claude Code and Codex unless the agent asks for longer. In OpenCode it lasts until its automation expires or is deleted.
- **Every wake costs a turn.** Filter in the watch code so only events that matter wake the session.
- **One Lark consumer per app.** A watch fails if `lark-cli event consume` or another copy of this server holds the app's connection.
- **Watch code has your credentials' access.** It can read anything they allow, start and cancel Codemagic builds, and create public artifact links. Grant only the scopes you need.
- **GitLab subscriptions are marked Experiment** and can change on a GitLab upgrade.
- **Codemagic push is undocumented.** It is the connection Codemagic's web app uses, and it can change without notice.

After you change credentials, run `opencode service restart` and reopen Claude Code and Codex.

The [reference](docs/reference.md) covers settings, client caveats, recipes, and troubleshooting.

## Release

1. Update `version` in the root `package.json`, then run these and commit the result:

   ```sh
   bun run sync:metadata
   bun install
   bun run format
   ```

2. Check that the package's trusted publisher on npm names this repository, the `publish-events-mcp.yml` workflow, and the `npm` environment, and allows staged publishing. A new trusted publisher expires unless it stages a release within 2 days.

3. Push a `v<version>` tag on that commit. The workflow runs the checks and stages the package under `latest`.

4. Inspect the staged release, then approve it with 2FA:

   ```sh
   npm stage list @luckuveryx/events-mcp
   npm stage download <stage-id>
   npm stage approve <stage-id>
   ```

Clients on `@latest` get the release at their next restart. To roll back, publish a higher version. Moving `latest` back does not replace a hub that already runs a newer version.
