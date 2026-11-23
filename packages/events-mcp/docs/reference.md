# events-mcp reference

The [README](../README.md) covers why and how to install. This page holds the details: every event, client notes, settings, limits, the watch workflow for agents, recipes, and troubleshooting.

## Events

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

Every GitLab event except `gitlab.userMergeRequestUpdated` needs a `target`. A target is a link on the configured GitLab host, a project path such as `group/project`, or a shorthand such as `group/project!123`, `group/project#123`, or `group/project@<full sha>`. The server reads the resource to prove the token can read it, then fills the subscription params. In Claude Code and Codex, `watch` returns the resolved params and `watches` lists them. `gitlab.userMergeRequestUpdated` subscribes as the token's user.

A target the token cannot read, or a link to a resource that does not exist, fails the watch before it subscribes.

GitLab marks the GraphQL subscription fields Experiment, so the selection set and arguments can change without deprecation. Regenerate the committed catalog after a GitLab upgrade:

```sh
GITLAB_TOKEN=... bun run --filter events-mcp generate:gitlab
```

## Client notes

### OpenCode

OpenCode arms a watch as an automation with an event trigger, so it needs `experimental.mcp_events`, and automations must not be turned off. The automation holds the watch's name and expiry, and `automation.list`, `pause`, `resume` and `delete` manage it. Without `expires` the watch has no time limit.

`"location_idle_timeout": false` stops OpenCode from unloading an idle location. Without it, OpenCode can end a long watch early. A numeric `location_idle_timeout` still unloads the location, and its watches, after that duration.

Put the block in the global config. Every session for this user shares one hub, with one Lark connection and one GitLab connection, so `"pool"` is unnecessary. OpenCode expands `{env:HOME}` but not `$HOME`.

### Claude Code

Claude Code receives wakes through MCP channels. The feature is a research preview. It needs claude.ai or Console API key auth and an organization that allows channels. It does not work with Bedrock, Google Cloud's Agent Platform, or Microsoft Foundry.

The channel confirmation appears on every launch during the preview. Without `--dangerously-load-development-channels`, Claude Code connects to the server but does not register the channel, so no wake arrives. The flag is ignored in `claude -p` and in the Agent SDK, so a headless session cannot receive wakes.

Claude Code queues channel messages while it is busy and delivers them together on the next turn. It sends no acknowledgement, so use `watches` to check that a watch is still armed.

The same server entry in a project `.mcp.json`, with the absolute path:

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

### Codex

Codex starts one MCP server process per thread and wakes the thread through its app-server control socket, `~/.codex/app-server-control/app-server-control.sock`. The `--no-daemon` mode has no control socket and cannot receive wakes. Codex must use the default home, `~/.codex`. A different Codex home is not supported.

Codex gives an MCP server only a fixed environment allowlist and does not pass your shell. `env_vars` forwards each named variable from the environment that started Codex, so export the credentials there and keep them out of the file. The `env` table holds literal values that stay in the file.

Codex does not tell an MCP server which thread it serves. The watch tool takes a required `threadId`, and the server's instructions tell the agent to read it from `$CODEX_THREAD_ID` in its own shell. The server checks the id with `thread/read` before the watch arms, so a missing or unknown id fails the watch instead of waking another session. A wake arrives as a new user turn in that thread.

## The hub

`events-mcp <harness>` connects to one hub per user and starts it when none is running.

- The hub keeps the environment of the session that started it, including `LARK_APP_ID`, `LARK_APP_SECRET`, and `GITLAB_TOKEN`. After you change those variables, end every session, Codex threads included, so the next one starts a new hub. A connected Codex keeps the old hub alive.
- Its socket is `$XDG_RUNTIME_DIR/events-hub/hub.sock`, or `/run/user/<uid>/events-hub/hub.sock` when `XDG_RUNTIME_DIR` is unset. On macOS it is `~/.local/state/events-hub/hub.sock`.
- Its log is `~/.local/state/events-hub/hub.log`.
- If the hub dies, its connections end and every open watch ends with them. Arm a new watch. Nothing re-arms on its own.

## Settings

No settings are required to start the server. Each source needs its own credentials. The decision model has a default.

| Variable                 | Default                                          | Effect                                                                                                                                                                                                                 |
| ------------------------ | ------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LARK_APP_ID`            | for Lark watches                                 | Lark app ID, `cli_` followed by 16 hex characters. Without it, Lark watches fail and GitLab watches work.                                                                                                              |
| `LARK_APP_SECRET`        | for Lark watches                                 | Lark app secret. Never write it into the config file.                                                                                                                                                                  |
| `GITLAB_TOKEN`           | unset                                            | GitLab token for the configured host. When unset or empty, the server reads `glab config get token --host <host>`. The token is validated with `GET /api/v4/user`, and a network failure is retried on the next watch. |
| `GITLAB_HOST`            | `gitlab.com`                                     | GitLab host for the Action Cable connection and the REST tool. Set it for self-managed GitLab. An empty value is an error.                                                                                             |
| `EVENTS_DECISIONS_MODEL` | `{"providerID":"opencode","id":"jev-1.13-free"}` | Model behind `tools.events.decide`, as JSON: `{"providerID":"your-provider","id":"your-model","variant":"optional"}`. Omit `variant` when unused. Each call sends one model request.                                   |

## Limits

- Everything is in memory. Watches, per-watch state, and the dedup cache live in the hub process. A hub restart starts fresh.
- In Claude Code and Codex, watch ids are shared by every session on the machine. `watches` lists every open watch, and `unwatch` can stop any of them. Neither covers OpenCode's automations. A watch still ends when the session that armed it disconnects. Sessions that stay connected share upstream subscriptions and dedup.
- In Claude Code and Codex, a watch stays open for 3600 seconds unless `expiresInSeconds` says otherwise. One event's code has a 120-second limit, and a watch handles one event at a time, so a slow event delays the ones behind it.
- Delivery is live only. Events that arrive while no watch is open, or while a connection reconnects, are dropped. The stream reports `cursor: null`, so there is no replay.
- Duplicate event IDs are dropped within each shared feed. Lark drops events older than an hour.
- OpenCode pauses the automation when its watch fails. The reason is in `lastOutput`, the session is not woken, and `automation.resume` arms it again. After a reconnect OpenCode arms the same watch again, and the automation keeps its state. Claude Code ends its watches when the session exits. Codex runs one server per thread, so a watch ends when that thread's server exits.
- Lark delivers each app's events to one WebSocket client. Before connecting, the server reads `/open-apis/event/v1/connection`. If another consumer is online, such as `lark-cli event consume` or this server on another machine, the watch fails with a clear error instead of splitting events.
- The GitLab notes channel is not part of the GraphQL schema and can change.
- In Claude Code and Codex, a wake is capped at 16,384 bytes, counted as `{ id, timestamp, data }`. Code that returns more stops the watch.
- Every wake starts a session turn and costs tokens.
- Every watch's code and every read tool can read whatever the credentials allow. Grant only the scopes your watches need. HR, payroll, and mail scopes expose personal data to the decision model.

## For agents: OpenCode

An automation reacts to each event. Its code action filters, keeps state and wakes the session.

1. Call `tools.opencode.list_mcp_events({ server: "events" })` in `execute` for the events, args and payloads.
2. Read the endpoints the code will read, with `lark_get` or `gitlab_get`, and confirm the response shape and IDs.
3. Create the automation.

```js
await tools.automation.create({
  name: "Failed pipelines",
  trigger: {
    type: "event",
    server: "events",
    event: "gitlab.ciPipelineStatusesUpdated",
    args: { target: "group/project" },
  },
  expires: "in 8 hours",
  action: {
    type: "code",
    code: `
const { id, status } = input.event.data;
if (status !== "FAILED") return;
return { send: { prompt: "Pipeline failed, untrusted data:\\n" + JSON.stringify(id) } };
`,
  },
});
```

The code reads `input.event = { id, timestamp, data }`, with the raw source payload in `data`, and `input.state`. It returns nothing to stay quiet, `{ state }` to remember, `{ send: { prompt } }` to wake the session, and `{ done: true }` to stop. It calls `tools.events.gitlab_get`, `tools.events.lark_get` and `tools.events.decide`, where `events` is the configured server name. Each run has 30 seconds. A prompt action with no `to` wakes the session on every event, and OpenCode skips one whose prompt and event exceed 32 KiB.

Manage it with `automation.list`, `pause`, `resume` and `delete`. The recipe for new comments is in the description of `gitlab.noteable.notes_changed`.

## For agents: Claude Code and Codex

These clients have no automations, so events-mcp runs the watch's code. A null result drops the event, and anything else wakes the session.

### Arm a watch

1. Call `list_events` for the events, targets and payloads.
2. Read the endpoints the code will read, with `lark_get` or `gitlab_get`, and confirm the response shape and IDs.
3. Call `watch({ "event": "<name>", "id": "<your name>", "code": "...", ... })`. Pass `target` when `list_events` lists one, and `expiresInSeconds` for more than an hour. Codex also passes `"threadId": "<the value of $CODEX_THREAD_ID>"`.

The wake text is the code's result: a string as is, or JSON as `JSON.stringify` of the value. In Claude Code it arrives as a channel message from the server, and in Codex as a new user turn in the same thread. To stop a watch early, call `unwatch({ "id": "..." })`. `watches` returns each active watch with the arguments it was armed with, which is what you need to arm it again, and the params a target resolved to. If a watch stops with `code failed`, fix the code and arm a new watch.

A watch's code is at most 64 KB and sees `input.event = { eventId, timestamp, data }`. The event `data` is the raw source payload.

Code that wakes on every event returns the data unchanged:

```js
return input.event.data;
```

Code that wakes only on some events returns null for the rest:

```js
if (input.event.data.status !== "FAILED") return null;
return input.event.data;
```

### Code tools

| Tool                                | What it does                                                 |
| ----------------------------------- | ------------------------------------------------------------ |
| `tools.lark.get({ path, query })`   | Read-only GET under `/open-apis`.                            |
| `tools.gitlab.get({ path, query })` | Read-only GET under `/api/v4`.                               |
| `tools.watch.state.get()`           | This watch's JSON memory, null until set.                    |
| `tools.watch.state.set(value)`      | Replace it, up to 64 KB.                                     |
| `tools.events.decide(request)`      | Decision model answers with probabilities, one per question. |

The Lark path rejects `..`, `//`, `%`, `\`, a query string, and a fragment. Pass query parameters in `query`, and page with `page_token` when the endpoint paginates. The GitLab path must stay under `/api/v4` after URL normalisation and rejects a redirect. Percent-encode project paths, for example `/projects/group%2Fproject`.

Use `decide` when plain code cannot tell whether an event deserves a wake. One call accepts several questions.

### Recipes

Each recipe is a watch for Claude Code and Codex, and also needs an `id`. In an OpenCode code action, use `tools.events.*_get` for reads, `input.state` and a returned `state` for memory, nothing instead of null, and `{ send: { prompt } }` instead of a result.

#### Babysit a failing pipeline

The watch wakes when any pipeline in `group/project` fails. The code drops every other status, reads the failed jobs, and hands both the pipeline and jobs to the session.

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
  "target": "group/project",
  "code": "…"
}
```

The payload's `id` is the pipeline global ID, and its numeric suffix is the pipeline database id. The jobs endpoint and the web URL `/pipelines/<n>` both take that database id. `iid` is project-local. `gitlab_get` returns JSON only, so read a failed job's log with `glab ci trace <job database id>` and retry it with `glab ci retry <job database id>`. The server never writes to GitLab.

Pass a pipeline link as `target` to follow one pipeline. Pass a commit link as `target` to follow that commit's pipelines. `gitlab.userMergeRequestUpdated` is the broad merge request event, and it takes no target.

#### Watch comments on a merge request

`gitlab.noteable.notes_changed` sends only `{"event":"updated"}`. The payload repeats the subscription ids and adds `noteableIid`. The code reads the newest 20 notes, including system notes, and compares their comment ids with `tools.watch.state`.

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
  "target": "group/project!123",
  "code": "…"
}
```

In Claude Code and Codex, `watch` returns the resolved `projectId`, `noteableType`, `noteableId`, and `noteableIid`.

A signal that finds nothing newer than the stored id returns null. If more than 20 comments arrive between two signals, the older ones in that burst are skipped.

#### Wake only for messages that need you

For `lark.im.message.receive_v1`, this code skips your own messages. It fetches the thread when `thread_id` is present, or the parent message when a reply has `parent_id` but no `thread_id`.

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

This code drops the event when the model is not confident.

### Verify

1. Run `opencode mcp list` or `claude mcp list`. For Codex, ask the agent in the TUI to call `list_events` instead. `codex mcp list` reports an entry as enabled even when its command does not exist, so it does not prove the server connected.
2. OpenCode calls `tools.opencode.list_mcp_events({ server: "events" })`. Claude Code and Codex call `list_events`. Each lists 22 events, including `lark.im.message.receive_v1` and `gitlab.noteable.notes_changed`.
3. For Lark, ask the user to trigger a subscribed event, such as a message to the bot. The wake confirms delivery.
4. For GitLab, call `gitlab_get` with `{ "path": "/user" }` to check the token. Then arm a watch on a project the user can read and wait for the next pipeline status change.
5. In Claude Code the wake appears as a channel message from the configured server, and in Codex as a new user turn in the watched thread. If it never arrives, check the client notes above.

## Troubleshoot

| Symptom                                                           | Cause                                                                                                                                                                                                                                                                  |
| ----------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server code changes do not take effect after a reload             | The hub process outlives client reloads. End every session, or kill the hub process. The next session starts a fresh one.                                                                                                                                              |
| Server fails to start                                             | The harness argument is missing or unknown, `GITLAB_HOST` is empty, `EVENTS_DECISIONS_MODEL` is not valid JSON in the model shape, or the committed event catalog is invalid. The harness argument is `opencode`, `claude`, or `codex`. Server logs go to the hub log. |
| Claude Code shows the server but no wake arrives                  | Start Claude Code with `MCP_PROTOCOL_NEGOTIATION=legacy` and `--dangerously-load-development-channels server:<name>`, using the name from `claude mcp list`. Channels also need claude.ai or Console API key auth.                                                     |
| A Lark watch fails to start with "No Lark app credentials"        | Set `LARK_APP_ID` and `LARK_APP_SECRET` for the service and arm a new watch.                                                                                                                                                                                           |
| Watch fails with "Another connection for this Lark app is online" | Another consumer of the same app holds a connection, such as `lark-cli event consume` or this server elsewhere. Stop it and arm a new watch.                                                                                                                           |
| Some events arrive, others do not                                 | The app lacks the subscription, or the event arrived while the connection was reconnecting.                                                                                                                                                                            |
| An event never arrives                                            | The app lacks the permission or subscription, or Lark does not deliver that event over WebSocket.                                                                                                                                                                      |
| A watch stops with `code failed` and `Lark API 99991672`          | The app lacks a read scope for the request.                                                                                                                                                                                                                            |
| A GitLab watch stops with "No GitLab token"                       | Set `GITLAB_TOKEN`, or authenticate `glab` for the GitLab host so `glab config get token --host <host>` returns a token.                                                                                                                                               |
| A GitLab watch fails to arm with "GitLab HTTP 404"                | The target names a resource the token cannot read, or the link is wrong. `gitlab_get` on the path in the message shows the same result.                                                                                                                                |
| A GitLab watch stops with "refused this watch"                    | GitLab refused the subscription after the target read succeeded. Check that the token can still read the resource, then arm a new watch.                                                                                                                               |
| A notes watch stops with "exceeds 16384 bytes"                    | The code returned more than one event may carry. Return less data.                                                                                                                                                                                                     |
| A Codex watch fails with "Codex does not know thread"             | `threadId` is wrong. Run `printf %s "$CODEX_THREAD_ID"` in the thread's shell and pass that exact value.                                                                                                                                                               |
| A Codex watch fails with "Cannot reach the Codex app-server"      | The Codex app-server control socket is not at `~/.codex/app-server-control/app-server-control.sock`. Start Codex without `--no-daemon`, using the default `~/.codex` home.                                                                                             |
| A watch disappears without a stop message                         | OpenCode reloaded, restarted, or unloaded the idle location, or the Claude Code or Codex session exited. For OpenCode, set `location_idle_timeout` to `false` and arm a new watch.                                                                                     |
| A watch stops with a timeout                                      | The event exceeded the code's 120-second limit. Shorten the work, such as fewer or smaller reads.                                                                                                                                                                      |
| A watch stops with "Watch state exceeds 65536 bytes"              | The code stores too much in `tools.watch.state`. Keep less, or derive it again on each event.                                                                                                                                                                          |
| Memory grows over time                                            | Watches and their buffers live in one long-running hub process, and buffers are unbounded. End the ones you no longer need with `unwatch`, or `automation.delete` in OpenCode.                                                                                         |
