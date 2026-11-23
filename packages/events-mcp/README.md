# events-mcp

events-mcp is a local MCP server that wakes an agent session in OpenCode, Claude Code, or Codex when something happens in Lark or GitLab. It connects to each integration, delivers the raw events a watch asks for, and gives the watch read tools and memory. The agent chooses what to watch and what deserves a wake-up.

## What you can watch

### Lark

All 14 events take no target and arrive over one shared WebSocket for the app.

| Event                                   | Fires when                                                     |
| --------------------------------------- | -------------------------------------------------------------- |
| `lark.im.message.receive_v1`            | The bot receives a message.                                    |
| `lark.im.message.reaction.created_v1`   | A reaction is added to a message.                              |
| `lark.im.message.reaction.deleted_v1`   | A reaction is removed from a message.                          |
| `lark.im.message.message_read_v1`       | A user reads a message the bot sent in a direct chat.          |
| `lark.drive.notice.comment_add_v1`      | A document comment or reply is added.                          |
| `lark.task.task.update_user_access_v2`  | A task is created, deleted, or changed, or its access changes. |
| `lark.application.bot.menu_v6`          | A user clicks a bot menu item.                                 |
| `lark.im.chat.updated_v1`               | Chat settings, owner, or permissions change.                   |
| `lark.im.chat.disbanded_v1`             | A chat is disbanded.                                           |
| `lark.im.chat.member.bot.added_v1`      | The bot joins a chat.                                          |
| `lark.im.chat.member.bot.deleted_v1`    | The bot leaves or is removed from a chat.                      |
| `lark.im.chat.member.user.added_v1`     | A user joins a chat.                                           |
| `lark.im.chat.member.user.deleted_v1`   | A user leaves or is removed from a chat.                       |
| `lark.im.chat.member.user.withdrawn_v1` | An invite to a chat is withdrawn.                              |

### GitLab

All 8 events arrive over one Action Cable connection to `/-/cable`.

| Event                                     | Target                                                                  | Payload                                                                  |
| ----------------------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `gitlab.ciPipelineStatusesUpdated`        | project path, project link, commit link, or `group/project@<full sha>`  | Any pipeline in the project or for the commit, on each status change.    |
| `gitlab.ciPipelineStatusUpdated`          | pipeline link                                                           | One pipeline, on each status change.                                     |
| `gitlab.ciJobStatusUpdated`               | job link                                                                | One job, on each status change.                                          |
| `gitlab.userMergeRequestUpdated`          | none                                                                    | A merge request whose author, assignee, or reviewer is the token's user. |
| `gitlab.mergeRequestMergeStatusUpdated`   | merge request link or `group/project!123`                               | A merge request, when its merge status changes.                          |
| `gitlab.mergeRequestReviewersUpdated`     | merge request link or `group/project!123`                               | A merge request, when its reviewers change.                              |
| `gitlab.mergeRequestApprovalStateUpdated` | merge request link or `group/project!123`                               | A merge request, when its approval state changes.                        |
| `gitlab.noteable.notes_changed`           | merge request or issue link, `group/project!123` or `group/project#123` | A merge request or issue, when any note changes.                         |

Every GitLab event except `gitlab.userMergeRequestUpdated` needs a `target`: a link on the configured GitLab host, a project path such as `group/project`, or a shorthand such as `group/project!123`, `group/project#123`, or `group/project@<full sha>`. The server reads the resource to prove the token can read it, then fills the subscription params. `watches` lists the resolved params beside the target, and in Claude Code and Codex `watch` also returns them. `gitlab.userMergeRequestUpdated` subscribes as the token's user.

## Setup

### Requirements

- Git, Bun 1.4.4, and OpenCode 2.3.2, Claude Code 2.1.330, or Codex 0.171.0.
- A Lark app in WebSocket event mode, with its app ID (`cli_` followed by 16 hex characters) and app secret.
- A GitLab personal access token for the GitLab host with `read_api` and `read_repository`, or `glab` authenticated for that host. The host defaults to `gitlab.com`.

The server reads events and integration data. The agent replies with its own tools, such as `lark-cli` or `glab`.

### Install

```sh
git clone https://github.com/LuckUVeryX/oc-plugins.git "$HOME/.local/share/events-mcp"
cd "$HOME/.local/share/events-mcp"
bun install --frozen-lockfile
```

Make Bun and the credentials below available to the service that runs the server. OpenCode and Claude Code read their own environment, which can differ from your shell.

### Configure OpenCode

Merge this into `~/.config/opencode/opencode.jsonc` and keep the existing settings.

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
          // For self-managed GitLab only:
          // "GITLAB_HOST": "gitlab.example.com",
        },
      },
    },
  },
}
```

If you cloned somewhere else, put that absolute path in `command` and `cwd`. OpenCode expands `{env:HOME}` but not `$HOME`.

`"location_idle_timeout": false` stops OpenCode from unloading an idle location. Without it, OpenCode can end a long watch before `expiresInSeconds`. A numeric `location_idle_timeout` still unloads the location, and its watches, after that duration.

Put the block in the global config. Every harness session for this user shares one hub, with one Lark connection and one GitLab connection, so `"pool"` is unnecessary.

End every session after you change the config or environment.

### Configure Claude Code

Claude Code receives wakes through MCP channels. The feature is a research preview. It needs claude.ai or Console API key auth and an organization that allows channels. It does not work with Bedrock, Google Cloud's Agent Platform, or Microsoft Foundry.

Start Claude Code from a shell that exports `LARK_APP_ID`, `LARK_APP_SECRET`, and `GITLAB_TOKEN` or the `glab` login. The hub inherits that environment. Add the server once, and pass `claude` as the harness:

```sh
claude mcp add --scope user events-mcp -- \
  bun "$HOME/.local/share/events-mcp/packages/events-mcp/src/main.ts" claude
```

The same entry in a project `.mcp.json`, with the absolute path:

```json
{
  "mcpServers": {
    "events-mcp": {
      "command": "bun",
      "args": ["/home/you/.local/share/events-mcp/packages/events-mcp/src/main.ts", "claude"]
    }
  }
}
```

Channels need the legacy MCP handshake, so start Claude Code with `MCP_PROTOCOL_NEGOTIATION=legacy` and allow the server as a development channel:

```sh
export MCP_PROTOCOL_NEGOTIATION=legacy
claude --dangerously-load-development-channels server:events-mcp
```

The channel confirmation appears on every launch during the preview. Without the flag, Claude Code connects to the server but does not register the channel, so no wake arrives. The flag is ignored in `claude -p` and in the Agent SDK, so a headless session cannot receive wakes.

Claude Code queues channel messages while it is busy and delivers them together on the next turn. It sends no acknowledgement, so use `watches` to check that a watch is still armed.

End every session after you change the config or environment.

### Configure Codex

Codex starts one MCP server process per thread and wakes the thread through its app-server control socket, `~/.codex/app-server-control/app-server-control.sock`. Run the Codex TUI normally, without `--no-daemon`. That mode has no control socket and cannot receive wakes.

Start Codex from a shell that exports `LARK_APP_ID`, `LARK_APP_SECRET`, and `GITLAB_TOKEN` or the `glab` login, plus `GITLAB_HOST` when the host is not `gitlab.com`. Add the server to `~/.codex/config.toml`, passing `codex` as the harness:

```toml
[mcp_servers.events]
command = "bun"
args = ["/home/you/.local/share/events-mcp/packages/events-mcp/src/main.ts", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 120
env_vars = ["LARK_APP_ID", "LARK_APP_SECRET", "GITLAB_TOKEN", "GITLAB_HOST"]
```

Codex gives an MCP server only a fixed environment allowlist and does not pass your shell. `env_vars` forwards each named variable from the environment that started Codex, so export the credentials there and keep them out of the file. The `env` table holds literal values that stay in the file.

Codex must use the default home, `~/.codex`; a different Codex home is not supported. The hub reads the app-server control socket there.

Codex does not tell an MCP server which thread it serves. The watch tool takes a required `threadId`, and the server's instructions tell the agent to read it from `$CODEX_THREAD_ID` in its own shell. The server checks the id with `thread/read` before the watch arms, so a missing or unknown id fails the watch instead of silently waking another session. A wake arrives as a new user turn in that thread, and the session acts on it.

End every session after you change the config or environment.

### The hub

`events-mcp <harness>` connects to one hub per user and starts it when none is running.

- It keeps the environment of the session that started it, including `LARK_APP_ID`, `LARK_APP_SECRET`, and `GITLAB_TOKEN`. After you change those variables, end every session, Codex threads included, so the next one starts a new hub. A connected Codex keeps the old hub alive.
- Its socket is `$XDG_RUNTIME_DIR/events-hub/hub.sock`, or `/run/user/<uid>/events-hub/hub.sock` when `XDG_RUNTIME_DIR` is unset. On macOS it is `~/.local/state/events-hub/hub.sock`.
- Its log is `~/.local/state/events-hub/hub.log`.
- If the hub dies, its connections end and every open watch ends with them. Arm a new watch; there is no re-arm.

### Settings

No settings are required to start the server. Each integration needs its own credentials. The decision model has a default and needs setting only to override it.

| Variable                 | Default                                          | Effect                                                                                                                                                                                                                 |
| ------------------------ | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LARK_APP_ID`            | for Lark watches                                 | Lark app ID, `cli_` followed by 16 hex characters. Without it, Lark watches fail and GitLab watches work.                                                                                                              |
| `LARK_APP_SECRET`        | for Lark watches                                 | Lark app secret. Never write it into the config file.                                                                                                                                                                  |
| `GITLAB_TOKEN`           | unset                                            | GitLab token for the configured host. When unset or empty, the server reads `glab config get token --host <host>`. The token is validated with `GET /api/v4/user`, and a network failure is retried on the next watch. |
| `GITLAB_HOST`            | `gitlab.com`                                     | GitLab host for the Action Cable connection and the REST tool. Set it for self-managed GitLab. An empty value is an error.                                                                                             |
| `EVENTS_DECISIONS_MODEL` | `{"providerID":"opencode","id":"jev-1.13-free"}` | Model behind `tools.events.decide`, as JSON: `{"providerID":"your-provider","id":"your-model","variant":"optional"}`. Omit `variant` when unused. Each call sends one model request.                                   |

## Limits

- Everything is in memory. Watches, per-watch state, and the dedup cache live in the hub process. A hub restart starts fresh.
- A watch belongs to the connection that armed it. When that OpenCode or Claude Code session disconnects, its watches end. Sessions that stay connected share upstream subscriptions and dedup.
- A watch stays open for 3600 seconds unless `expiresInSeconds` says otherwise. One event's script has a 120-second limit, and a watch handles one event at a time, so a slow event delays the ones behind it.
- Delivery is live only. Events that arrive while no watch is open, or while a connection reconnects, are dropped. The stream reports `cursor: null`, so there is no replay.
- Duplicate event IDs are dropped within each shared feed. Lark drops events older than an hour.
- OpenCode ends every watch without a stop message when it reloads, restarts, or unloads an idle location. Set `location_idle_timeout` to `false` for long watches. Claude Code ends them when the session exits. Codex runs one server per thread, so a watch ends when that thread's server exits.
- Lark delivers each app's events to one WebSocket client. Before connecting, the server reads `/open-apis/event/v1/connection`. If another consumer is online, such as `lark-cli event consume` or this server on another machine, the watch fails with a clear error instead of splitting events.
- GitLab marks the GraphQL subscription fields Experiment. The notes channel is not part of the GraphQL schema and can also change.
- A GitLab watch reads its target before it subscribes, so a missing permission fails at arm time.
- A delivered event is capped at 16,384 bytes, counted as `{ id, timestamp, data }`. A script that returns more stops the watch.
- Every wake starts a session turn and costs tokens. A watch stays open until it expires or the agent stops it.
- Every script and every read tool can read whatever the credentials allow. Grant only the scopes your watches need. HR, payroll, and mail scopes expose personal data to the decision model.

## For agents: setting up a watch

This section is the whole workflow. OpenCode calls `watch_mcp_event`. Claude Code and Codex call `list_events`, then `watch`. All of them take the same watch arguments, and Codex also takes a `threadId`.

Each event runs the watch's script. A null result drops the event, and anything else wakes the session.

### Arm a watch

1. OpenCode calls `watch_mcp_event({ "server": "events" })`. Claude Code and Codex call `list_events`. All of them list the events and each argument's schema and description; read the arguments there, not here.
2. Read the same endpoints the script will read, with `lark_get` or `gitlab_get`, and confirm the response shape and IDs.
3. OpenCode calls `watch_mcp_event({ "server": "events", "event": "<name>", "arguments": { ... } })`. Claude Code and Codex call `watch({ "event": "<name>", ... })` with the same fields. For GitLab, pass `target` when `list_events` lists one. `watches` shows the resolved params in every harness, and in Claude Code and Codex `watch` also returns them. Codex also passes `"threadId": "<the value of $CODEX_THREAD_ID>"`.

A watch stays open until it expires or you stop it. Every event the script does not drop wakes you.

The wake text is the script's result: a string as is, or JSON as `JSON.stringify` of the value. In Claude Code it arrives as a channel message from the server, and in Codex as a new user turn in the same thread. To stop a watch early, call `unwatch({ "watchId": "..." })`. `watches` returns each active watch with the arguments it was armed with, which is what you need to arm it again, and the params a target resolved to. If a watch stops with `script failed`, fix the script and arm a new watch.

A script is at most 64 KB and sees `input.event = { eventId, timestamp, data }`. The event `data` is the raw integration payload.

A script that wakes on every event returns the data unchanged:

```js
return input.event.data;
```

A script that wakes only on some events returns null for the rest:

```js
if (input.event.data.status !== "FAILED") return null;
return input.event.data;
```

### Script tools

| Tool                                | Present when | What it does                                                 |
| ----------------------------------- | ------------ | ------------------------------------------------------------ |
| `tools.lark.get({ path, query })`   | always       | Read-only GET under `/open-apis`.                            |
| `tools.gitlab.get({ path, query })` | always       | Read-only GET under `/api/v4`.                               |
| `tools.watch.state.get()`           | always       | This watch's JSON memory; null until set.                    |
| `tools.watch.state.set(value)`      | always       | Replace it, up to 64 KB.                                     |
| `tools.events.decide(request)`      | always       | Decision model answers with probabilities, one per question. |

The Lark path rejects `..`, `//`, `%`, `\`, a query string, and a fragment. Pass query parameters in `query`, and page with `page_token` when the endpoint paginates. The GitLab path must stay under `/api/v4` after URL normalisation and rejects a redirect. Percent-encode project paths, for example `/projects/group%2Fproject`.

Use `decide` in the script when plain code cannot tell whether an event deserves a wake. One call accepts several questions.

### GitLab targets

Arm a GitLab watch from a link or shorthand with `target`. Pass a merge request link such as `https://gitlab.example.com/group/project/-/merge_requests/123`, an issue link, a pipeline link, a job link, a commit link, a project path such as `group/project`, or a shorthand such as `group/project!123`, `group/project#123`, or `group/project@<full sha>`. The server reads the resource to prove access and to fill the params. `watches` lists those params, and in Claude Code and Codex `watch` also returns them.

A target the token cannot read, or a link to a resource that does not exist, fails the watch before it subscribes.

The GraphQL subscription fields are Experiment, so the selection set and arguments can change without deprecation. Regenerate the committed catalog after a GitLab upgrade.

```sh
GITLAB_TOKEN=... bun run --filter events-mcp generate:gitlab
```

### Recipes

Each recipe is the event plus the arguments to pass with it.

#### Babysit a failing pipeline

The watch wakes when any pipeline in `group/project` fails. The script drops every other status, reads the failed jobs, and hands both the pipeline and jobs to the session.

```js
const { id, project, status } = input.event.data;
if (status !== "FAILED") return null;
const pipelineId = id.split("/").pop();
const jobs = await tools.gitlab.get({
  path: `/projects/${encodeURIComponent(project.fullPath)}/pipelines/${pipelineId}/jobs`,
  query: { scope: "failed" },
});
return { pipeline: input.event.data, jobs };
```

```json
{
  "watchId": "pipeline",
  "target": "group/project",
  "script": "…"
}
```

The payload's `id` is the pipeline global ID, and its numeric suffix is the pipeline database id. The jobs endpoint and the web URL `/pipelines/<n>` both take that database id; `iid` is project-local. `gitlab_get` returns JSON only, so read a failed job's log with `glab ci trace <job database id>` and retry it with `glab ci retry <job database id>`. The server never writes to GitLab.

Pass a pipeline link as `target` to follow one pipeline. Pass a commit link as `target` to follow that commit's pipelines. `gitlab.userMergeRequestUpdated` is the broad merge request event, and it takes no target.

#### Watch comments on a merge request

`gitlab.noteable.notes_changed` sends only `{"event":"updated"}`. The payload repeats the subscription ids and adds `noteableIid`. The script reads the newest 20 notes, including system notes, and compares their comment ids with `tools.watch.state`.

Before arming, set `START` to the newest non-system note id from `GET /projects/:id/merge_requests/:iid/notes?sort=desc&order_by=created_at&per_page=20`, or the issues equivalent. Earlier comments are not replayed and the first new comment is delivered.

```js
const START = 0;
const { projectId, noteableType, noteableIid } = input.event.data;
const collection = noteableType === "merge_request" ? "merge_requests" : "issues";
const notes = await tools.gitlab.get({
  path: "/projects/" + projectId + "/" + collection + "/" + noteableIid + "/notes",
  query: { sort: "desc", order_by: "created_at", per_page: 20 },
});
const comments = notes.filter((note) => !note.system);
const previous = (await tools.watch.state.get()) ?? START;
const fresh = comments.filter((note) => note.id > previous);
await tools.watch.state.set(comments.reduce((max, note) => Math.max(max, note.id), previous));
return fresh.length === 0 ? null : { notes: fresh };
```

```json
{
  "watchId": "mr-comments",
  "target": "group/project!123",
  "script": "…"
}
```

`watches` lists the resolved `projectId`, `noteableType`, `noteableId`, and `noteableIid`, and in Claude Code and Codex `watch` also returns them.

A signal that finds nothing newer than the stored id returns null. If more than 20 comments arrive between two signals, the older ones in that burst are skipped.

#### Wake only for messages that need you

For `lark.im.message.receive_v1`, this script skips your own messages. It fetches the thread when `thread_id` is present, or the parent message when a reply has `parent_id` but no `thread_id`.

```js
const { sender, message } = input.event.data;
if (sender?.sender_id?.open_id === "ou_your_open_id") return null;
let context = null;
if (message.thread_id) {
  context = await tools.lark.get({
    path: "/im/v1/messages",
    query: { container_id_type: "thread", container_id: message.thread_id },
  });
} else if (message.parent_id) {
  context = await tools.lark.get({
    path: `/im/v1/messages/${message.parent_id}`,
  });
}
return { message, context };
```

Thread IDs start with `omt_`. A returned message's `body.content` is a JSON string. Parse it when you need its fields.

A message can be a reply such as "be creative" that only matters because of the thread. Give the model the thread and tell it who you are. Replace the final `return` with this:

```js
const { answers } = await tools.events.decide({
  input: [
    {
      type: "text",
      text: "I am <your name>. A message needs me if it names me, asks me for something, or continues a conversation that asked me for something. context holds the earlier messages.",
    },
    { type: "json", value: { message, context } },
  ],
  questions: [
    {
      type: "predicate",
      name: "needs_me",
      instructions: "Does the latest message need me?",
      criteria: {
        true: "It names me, asks me for something, or continues a request made to me.",
        false: "It is unrelated to me.",
      },
    },
  ],
});
const answer = answers.find((item) => item.name === "needs_me");
if (!answer || answer.type !== "predicate" || answer.probability < 0.7) return null;
return { message, context, answers };
```

This script drops the event when the model is not confident.

### Verify

1. Run `opencode mcp list` or `claude mcp list`. For Codex, ask the agent in the TUI to call `list_events` instead. `codex mcp list` reports an entry as enabled even when its command does not exist, so it does not prove the server connected.
2. OpenCode calls `watch_mcp_event` with server `events` and no event. Claude Code and Codex call `list_events`. Each lists 22 events, including `lark.im.message.receive_v1` and `gitlab.noteable.notes_changed`.
3. For Lark, ask the user to trigger a subscribed event, such as a message to the bot. The wake confirms delivery.
4. For GitLab, call `gitlab_get` with `{ "path": "/user" }` to check the token. Then arm a watch on a project the user can read and wait for the next pipeline status change.
5. In Claude Code the wake appears as a channel message from the configured server, and in Codex as a new user turn in the watched thread. If it never arrives, check the setup for that client below.

### Troubleshoot

| Symptom                                                           | Cause                                                                                                                                                                                                                                                                                |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Server code changes do not take effect after a reload             | The hub process outlives harness reloads. End every session, or kill the hub process; the next session starts a fresh one.                                                                                                                                                           |
| Server fails to start                                             | The harness argument is missing or unknown (use `events-mcp opencode`, `events-mcp claude`, or `events-mcp codex`), `GITLAB_HOST` is empty, `EVENTS_DECISIONS_MODEL` is not valid JSON in the model shape, or the committed event catalog is invalid. Server logs go to the hub log. |
| Claude Code shows the server but no wake arrives                  | Start Claude Code with `MCP_PROTOCOL_NEGOTIATION=legacy` and `--dangerously-load-development-channels server:<name>`, using the name from `claude mcp list`. Channels also need claude.ai or Console API key auth.                                                                   |
| A Lark watch fails to start with "No Lark app credentials"        | Set `LARK_APP_ID` and `LARK_APP_SECRET` for the service and arm a new watch.                                                                                                                                                                                                         |
| Watch fails with "Another connection for this Lark app is online" | Another consumer of the same app holds a connection, such as `lark-cli event consume` or this server elsewhere. Stop it and arm a new watch.                                                                                                                                         |
| Some events arrive, others do not                                 | The app lacks the subscription, or the event arrived while the connection was reconnecting.                                                                                                                                                                                          |
| An event never arrives                                            | The app lacks the permission or subscription, or Lark does not deliver that event over WebSocket.                                                                                                                                                                                    |
| A watch stops with `script failed` and `Lark API 99991672`        | The app lacks a read scope for the request.                                                                                                                                                                                                                                          |
| A GitLab watch stops with "No GitLab token"                       | Set `GITLAB_TOKEN`, or authenticate `glab` for the GitLab host so `glab config get token --host <host>` returns a token.                                                                                                                                                             |
| A GitLab watch fails to arm with "GitLab HTTP 404"                | The target names a resource the token cannot read, or the link is wrong. `gitlab_get` on the path in the message shows the same result.                                                                                                                                              |
| A GitLab watch stops with "refused this watch"                    | GitLab refused the subscription after the target read succeeded. Check that the token can still read the resource, then arm a new watch.                                                                                                                                             |
| A notes watch stops with "exceeds 16384 bytes"                    | The script returned an event bigger than OpenCode accepts. Return less data.                                                                                                                                                                                                         |
| A Codex watch fails with "Codex does not know thread"             | `threadId` is wrong. Run `printf %s "$CODEX_THREAD_ID"` in the thread's shell and pass that exact value.                                                                                                                                                                             |
| A Codex watch fails with "Cannot reach the Codex app-server"      | The Codex app-server control socket is not at `~/.codex/app-server-control/app-server-control.sock`. Start Codex without `--no-daemon`, using the default `~/.codex` home; a different Codex home is not supported.                                                                  |
| A watch disappears without a stop message                         | OpenCode reloaded, restarted, or unloaded the idle location, or the Claude Code or Codex session exited. For OpenCode, set `location_idle_timeout` to `false` and arm a new watch.                                                                                                   |
| A watch stops with a timeout                                      | The event exceeded the script's 120-second limit. Shorten the work, such as fewer or smaller reads.                                                                                                                                                                                  |
| A watch stops with "Watch state exceeds 65536 bytes"              | The script stores too much in `tools.watch.state`. Keep less, or derive it again on each event.                                                                                                                                                                                      |
| Memory grows over time                                            | Watches and their buffers live in one long-running hub process, and buffers are unbounded. Call `watches`, then `unwatch` the ones you no longer need.                                                                                                                               |

### Update

Run this at the repository root, then end every session, Codex threads included, so the next session starts a new hub.

```sh
git pull --ff-only
bun install --frozen-lockfile
```
