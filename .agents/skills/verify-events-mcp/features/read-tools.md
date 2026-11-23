# Read tools

The agent and every watch's code can read GitLab and Lark through `gitlab_get` and `lark_get`. Both are read-only GETs under a fixed API prefix, so a watch can fetch the context a raw event omits.

## Sub-features

- `tool-gitlab-user` reads `/user` to prove the token and identify the calling account.
- `tool-gitlab-project` resolves a project path and returns its id, namespace, and default branch.
- `tool-lark-chat` reads a chat through `/im/v1/chats/<chat id>` when Lark credentials are set.
- `tool-list-stop` lists and stops watches in Claude Code and Codex through `watches` and `unwatch`. A watch armed with `target` is listed with its resolved `params`.
- `tool-reject` refuses a path outside the prefix or with a query string, fragment, or traversal.

## How to get to it (user POV)

- Ask the agent to read a GitLab resource, or write `tools.gitlab.get({ path, query })` in a watch's code.
- Ask for a Lark resource the same way with `tools.lark.get({ path, query })`.
- The tools appear in the session's tool list as `gitlab_get` and `lark_get` next to `watches` and `unwatch` in Claude Code and Codex.

## Driving it with mcp-client.mjs

Preconditions:

- `bun scripts/target.ts doctor` reports zero failures.
- `VERIFY_GITLAB_PROJECT` names the project the token can read.
- For the Lark leg, `VERIFY_LARK_APP_ID`, `VERIFY_LARK_APP_SECRET`, and `VERIFY_LARK_CHAT_ID` are set.

- **GitLab user.** Run `bun scripts/mcp-client.mjs read-tools`. It calls `gitlab_get` with `{ "path": "/user" }` and prints `{ "id": <n>, "username": "...", "state": "active" }`. The username proves the token belongs to a real account.
- **GitLab project.** The same command reads `/projects/<url encoded project>` and prints `path_with_namespace` equal to `$VERIFY_GITLAB_PROJECT` plus `default_branch`. A 401 or 404 fails the command.
- **Lark chat.** With Lark variables set, it also calls `lark_get` with `{ "path": "/im/v1/chats/<chat id>" }` and prints `"lark": { "chat_mode": "p2p" }`. `lark_get` returns the envelope's `data` only, so there is no `code`. Any mode other than `p2p` fails the command. Without the variables the record has `"lark": null` and the command still passes the GitLab legs.
- **Path rejection.** Call the tool with a query string and expect an error. From a session, ask the agent to call `gitlab_get` with `{ "path": "/user?x=1" }`; the tool answers with `Invalid GitLab REST path. Use a path relative to /api/v4, such as /projects/group%2Fproject/pipelines, without .., //, a query string or a fragment.`, not data. The [Claude Code channel](./claude-channel.md) recipe drives this step.
- **List and stop.** `read-tools` does not call `watches` or `unwatch`. The Stop step of the [Claude Code channel](./claude-channel.md) recipe covers both.
- **Evidence.** `read-tools.json` holds the user, project, and Lark records. It never contains the token or the app secret.
- **Cleanup.** Run `scripts/run.sh cleanup "$VERIFY_RUN_DIR"`.

## Gotchas

- `gitlab_get` paths stay under `/api/v4` after normalisation and reject a redirect. Percent-encode a project path, for example `/projects/group%2Fproject`.
- `lark_get` paths stay under `/open-apis` and reject `..`, `//`, `%`, a backslash, a query string, and a fragment. Pass query parameters in `query`.
- Both tools return JSON only. A job log or a binary body is not readable; use `glab ci trace` outside the server.
- Lark endpoints paginate. Pass `page_token` in `query`; the tool does not follow pages for you.
- A resource the token cannot read fails with a message that names it, and GitLab sometimes answers 404 for a resource that exists. Check with the same call before blaming the watch.
- A watch's code sees the same tools, so a read tool failure fails the code, not the event feed.
