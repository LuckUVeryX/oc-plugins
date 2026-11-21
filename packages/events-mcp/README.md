# events-mcp

events-mcp is a local MCP server that wakes an OpenCode session when something happens in Lark or GitLab. It connects to each source, delivers the raw events a watch asks for, and gives the watch read tools and memory. The agent chooses what to watch and what deserves a wake-up.

## What you can watch

### Lark

All 14 events take `params: {}` and arrive over one shared WebSocket for the app.

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

| Event                                     | Params                                           | Payload                                                      |
| ----------------------------------------- | ------------------------------------------------ | ------------------------------------------------------------ |
| `gitlab.ciPipelineStatusesUpdated`        | `projectFullPath` or `projectId`, optional `sha` | Any pipeline in the project, on each status change.          |
| `gitlab.ciPipelineStatusUpdated`          | `pipelineId`                                     | One pipeline, on each status change.                         |
| `gitlab.ciJobStatusUpdated`               | `jobId`                                          | One job, on each status change.                              |
| `gitlab.userMergeRequestUpdated`          | `userId`                                         | A merge request assigned to or reviewed by the token's user. |
| `gitlab.mergeRequestMergeStatusUpdated`   | `issuableId`                                     | A merge request, when its merge status changes.              |
| `gitlab.mergeRequestReviewersUpdated`     | `issuableId`                                     | A merge request, when its reviewers change.                  |
| `gitlab.mergeRequestApprovalStateUpdated` | `issuableId`                                     | A merge request, when its approval state changes.            |
| `gitlab.noteable.notes_changed`           | `projectId`, `noteableType`, `noteableId`        | A merge request or issue, when any note changes.             |

## Setup

### Requirements

- Git, Bun 1.4.4, and OpenCode 2.3.2.
- A Lark app in WebSocket event mode, with its app ID (`cli_` followed by 16 hex characters) and app secret.
- A GitLab personal access token for the host with `read_api` and `read_repository`, or `glab` authenticated for that host.

The server reads events and source data. The agent replies with its own tools, such as `lark-cli` or `glab`.

### Install

```sh
git clone https://github.com/LuckUVeryX/oc-plugins.git "$HOME/.local/share/events-mcp"
cd "$HOME/.local/share/events-mcp"
bun install --frozen-lockfile
```

Make Bun, `LARK_APP_ID`, `LARK_APP_SECRET`, and either `GITLAB_TOKEN` or an authenticated `glab` available to the OpenCode service. OpenCode reads its own environment, which can differ from your shell.

### Configure OpenCode

Merge this into `~/.config/opencode/opencode.jsonc` and keep the existing settings.

```jsonc
{
  "experimental": { "mcp_events": true, "location_idle_timeout": false },
  "mcp": {
    "servers": {
      "events": {
        "type": "local",
        "command": ["bun", "{env:HOME}/.local/share/events-mcp/packages/events-mcp/src/main.ts"],
        "cwd": "{env:HOME}/.local/share/events-mcp",
        "pool": true,
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

If you cloned somewhere else, put that absolute path in `command` and `cwd`. OpenCode expands `{env:HOME}` but not `$HOME`.

`"location_idle_timeout": false` stops OpenCode from unloading an idle location. Without it, OpenCode can end a long watch before `expiresInSeconds`. A numeric `location_idle_timeout` still unloads the location, and its watches, after that duration.

Put the block in the global config. With `"pool": true`, every project in one OpenCode service shares one server process, one Lark connection, and one GitLab connection.

Restart the OpenCode service after you change the config or environment.

### Settings

| Variable                       | Default             | Effect                                                                                                                                                                                                                                    |
| ------------------------------ | ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LARK_APP_ID`                  | for Lark watches    | Lark app ID, `cli_` followed by 16 hex characters. Without it, Lark watches fail and GitLab watches work.                                                                                                                                 |
| `LARK_APP_SECRET`              | for Lark watches    | Lark app secret. Never write it into the config file.                                                                                                                                                                                     |
| `GITLAB_TOKEN`                 | unset               | GitLab token. When unset or empty, the server reads `glab config get token --host <host>`. The token is validated with `GET /api/v4/user`, and a network failure is retried on the next watch.                                            |
| `GITLAB_HOST`                  | `gitlab.example.com` | Host for the Action Cable connection and the REST tool. An empty value is an error.                                                                                                                                                       |
| `EVENTS_DECISIONS_MODEL`       | unset               | Model behind `tools.events.decide`, as JSON: `{"providerID":"your-provider","id":"your-model","variant":"optional"}`. Omit `variant` when unused. Without this variable, prepare has no `decide` tool. Each call sends one model request. |
| `EVENTS_WATCH_DEFAULT_SECONDS` | `3600`              | Watch lifetime when `expiresInSeconds` is omitted. Positive integer.                                                                                                                                                                      |
| `EVENTS_STEP_TIMEOUT_SECONDS`  | `120`               | Time limit for one event's prepare and transform together. Positive integer. A watch handles one event at a time, so a slow event delays the ones behind it.                                                                              |

## Limits

- Everything is in memory. Watches, per-watch state, and the dedup cache live in the server process. A restart starts fresh.
- Delivery is live only. Events that arrive while no watch is open, or while a connection reconnects, are dropped. The stream reports `cursor: null`, so there is no replay.
- Duplicate event IDs are dropped within each shared subscription. Lark drops events older than an hour.
- OpenCode ends every watch without a stop message when it reloads, restarts, or unloads an idle location. Set `location_idle_timeout` to `false` for long watches.
- Lark delivers each app's events to one WebSocket client. Before connecting, the server reads `/open-apis/event/v1/connection`. If another consumer is online, such as `lark-cli event consume` or this server on another machine, the watch fails with a clear error instead of splitting events.
- GitLab marks the GraphQL subscription fields Experiment. The notes channel is not part of the GraphQL schema and can also change.
- GitLab access checks cover projects, merge requests, issues, and users. A watch on a pipeline or job ID alone stays silent when the token cannot read it.
- A delivered event is capped at 16,384 bytes, counted as `{ id, timestamp, data }`. A transform that returns more stops the watch.
- Every wake starts a session turn and costs tokens. A watch stops after its first wake unless the agent passes `"once": false`.
- Every prepare script and every read tool can read whatever the credentials allow. Grant only the scopes your watches need. HR, payroll, and mail scopes expose personal data to the decision model.

## For agents: setting up a watch

This section is the whole workflow. It uses OpenCode 2.3.2's watch tool; the server speaks the same events protocol to any client.

Each event passes filter → prepare → transform, and transform's string wakes the session.

### Arm a watch

1. Call `watch_mcp_event({ "server": "events" })`. It lists the events and each argument's schema and description; read the arguments there, not here.
2. Read the same endpoints prepare will read, with `lark_get` or `gitlab_get`, and confirm the response shape and IDs.
3. Call `watch_mcp_event({ "server": "events", "event": "<name>", "arguments": { ... } })`.

A watch stops after its first wake unless you pass `"once": false`.

The wake text carries the string transform returned. To stop a watch early, call `stop_watch({ "watchId": "..." })`. `list_watches` returns each active watch with the arguments it was armed with, which is what you need to arm it again. If a watch stops with `<step> failed`, fix that step and arm a new watch.

Each script is at most 64 KB, and both see `input.event = { eventId, timestamp, data }`. The event `data` is the raw upstream payload.

A typical watch returns the data unchanged:

```js
// prepare
return input.event.data;

// transform
return JSON.stringify(input.prepared);
```

### Prepare tools

| Tool                                | Present when                    | What it does                                                 |
| ----------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| `tools.lark.get({ path, query })`   | always                          | Read-only GET under `/open-apis`.                            |
| `tools.gitlab.get({ path, query })` | always                          | Read-only GET under `/api/v4`.                               |
| `tools.watch.state.get()`           | always                          | This watch's JSON memory; null until set.                    |
| `tools.watch.state.set(value)`      | always                          | Replace it, up to 64 KB.                                     |
| `tools.events.decide(request)`      | `EVENTS_DECISIONS_MODEL` is set | Decision model answers with probabilities, one per question. |

The Lark path rejects `..`, `//`, `%`, `\`, a query string, and a fragment. Pass query parameters in `query`, and page with `page_token` when the endpoint paginates. The GitLab path must stay under `/api/v4` after URL normalisation and rejects a redirect. Percent-encode project paths, for example `/projects/group%2Fproject`.

Use `decide` in prepare when `filter` cannot tell whether an event deserves a wake. One call accepts several questions.

### GitLab IDs

Global IDs look like `gid://gitlab/MergeRequest/1234` and are not the numbers in the web URL. `gitlab.noteable.notes_changed` takes database ids instead. Each event's params schema carries its ID format.

The params schema rejects a missing or malformed ID before the watch subscribes, and a resource the token cannot read fails with a message that names it.

The GraphQL subscription fields are Experiment, so the selection set and arguments can change without deprecation. Regenerate the committed catalog after a GitLab upgrade.

```sh
GITLAB_TOKEN=... bun run --filter events-mcp generate:gitlab
```

### Recipes

Each recipe is the event plus the arguments to pass with it.

#### Babysit a failing pipeline

The watch wakes when any pipeline in `group/project` fails. Prepare reads the failed jobs and transform hands both the pipeline and jobs to the session.

```js
// prepare
const { id, project } = input.event.data;
const pipelineId = id.split("/").pop();
const jobs = await tools.gitlab.get({
  path: `/projects/${encodeURIComponent(project.fullPath)}/pipelines/${pipelineId}/jobs`,
  query: { scope: "failed" },
});
return { pipeline: input.event.data, jobs };
```

```js
// transform
return JSON.stringify(input.prepared);
```

```json
{
  "watchId": "otp-pipeline",
  "params": { "projectFullPath": "group/project" },
  "filter": { "status": ["FAILED"] },
  "prepare": "…",
  "transform": "return JSON.stringify(input.prepared)"
}
```

The payload's `id` is the pipeline global ID, and its numeric suffix is the pipeline database id. The jobs endpoint and the web URL `/pipelines/<n>` both take that database id; `iid` is project-local. `gitlab_get` returns JSON only, so read a failed job's log with `glab ci trace <job database id>` and retry it with `glab ci retry <job database id>`. The server never writes to GitLab.

Pass `pipelineId` to follow one pipeline. Add `sha` to `ciPipelineStatusesUpdated` to follow one commit. `gitlab.userMergeRequestUpdated` is the broad merge request event, and `userId` must be the token's user.

#### Watch comments on a merge request

`gitlab.noteable.notes_changed` sends only `{"event":"updated"}`. The payload repeats the subscription ids and adds `noteableIid`. Prepare reads the newest 20 notes, including system notes, and compares their comment ids with `tools.watch.state`.

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
  "params": { "projectId": 599, "noteableType": "merge_request", "noteableId": 48090 },
  "prepare": "…",
  "transform": "return JSON.stringify(input.prepared)"
}
```

A signal that finds nothing newer than the stored id returns null. If more than 20 comments arrive between two signals, the older ones in that burst are skipped.

#### Wake only for messages that need you

For `lark.im.message.receive_v1`, this prepare skips your own messages. It fetches the thread when `thread_id` is present, or the parent message when a reply has `parent_id` but no `thread_id`.

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

This prepare drops the event when the model is not confident. The transform stays the pass-through.

```js
return JSON.stringify(input.prepared);
```

### Verify

1. Run `opencode mcp list`. The `events` server should show as connected. This checks the MCP connection only.
2. Call `watch_mcp_event` with server `events` and no event. It should list 22 events, including `lark.im.message.receive_v1` and `gitlab.noteable.notes_changed`.
3. For Lark, ask the user to trigger a subscribed event, such as a message to the bot. The wake confirms delivery.
4. For GitLab, call `gitlab_get` with `{ "path": "/user" }` to check the token. Then arm a watch on a project the user can read and wait for the next pipeline status change.

### Troubleshoot

| Symptom                                                           | Cause                                                                                                                                           |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Server code changes do not take effect after a reload             | With `pool: true`, the server process outlives OpenCode reloads. Stop the `events-mcp` process; OpenCode starts a fresh one on the next reload. |
| Server fails to start                                             | `GITLAB_HOST` is empty, or the committed event catalog is invalid. Server logs go to stderr.                                                    |
| A Lark watch stops with "No Lark app credentials"                 | Set `LARK_APP_ID` and `LARK_APP_SECRET` for the OpenCode service and arm a new watch.                                                           |
| Watch fails with "Another connection for this Lark app is online" | Another consumer of the same app holds a connection, such as `lark-cli event consume` or this server elsewhere. Stop it and arm a new watch.    |
| Some events arrive, others do not                                 | The app lacks the subscription, or the event arrived while the connection was reconnecting.                                                     |
| An event never arrives                                            | The app lacks the permission or subscription, or Lark does not deliver that event over WebSocket.                                               |
| A watch stops with `prepare failed` and `Lark API 99991672`       | The app lacks a read scope for the request.                                                                                                     |
| A GitLab watch stops with "No GitLab token"                       | Set `GITLAB_TOKEN`, or authenticate `glab` for the host so `glab config get token --host <host>` returns a token.                               |
| A GitLab watch stops with "is not accessible"                     | The token cannot read that project or merge request, or the global ID is wrong. `gitlab_get` with `/projects/<id>` shows the same result.       |
| A GitLab pipeline or job watch stays silent                       | The token cannot read that pipeline or job, and GitLab gives no lookup by global ID alone.                                                      |
| A GitLab watch stops with "Unauthorized subscription"             | `userId` is not the token's user, or the merge request is outside the token's access.                                                           |
| A notes watch stops with "rejected the subscription"              | `projectId` and `noteableId` do not belong together, or the token cannot read that project or noteable.                                         |
| A notes watch stops with "exceeds 16384 bytes"                    | Transform returned an event bigger than OpenCode accepts. Return less data.                                                                     |
| A watch stops with a timeout                                      | The event exceeded `EVENTS_STEP_TIMEOUT_SECONDS`. Inspect the named step or increase the limit.                                                 |
| A watch stops with "Watch state exceeds 65536 bytes"              | Prepare stores too much in `tools.watch.state`. Keep less, or derive it again on each event.                                                    |
| A watch disappears without a stop message                         | OpenCode reloaded, restarted, or unloaded the idle location. Set `location_idle_timeout` to `false` and arm a new watch.                        |
| Memory grows over time                                            | Watches and their buffers live in one long-running server process, and buffers are unbounded. Run `list_watches` and stop unused watches.       |

### Update

Run this at the repository root, then restart the OpenCode service.

```sh
git pull --ff-only
bun install --frozen-lockfile
```
