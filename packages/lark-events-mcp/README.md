# lark-events-mcp

lark-events-mcp is a local MCP server that sends Lark events to OpenCode. An agent opens a watch on one event, such as `im.message.receive_v1`, and gets one notification for each matching event until the watch stops or expires.

```mermaid
flowchart LR
    Lark[Lark app] -- WebSocket --> Server[lark-events-mcp]
    Server -- MCP event notifications --> OpenCode
    OpenCode --> Agent
```

## Why use it

Without this server, an agent learns about new Lark activity only when you paste it in or ask it to poll. With a watch, the session that opened it receives each event as it happens. Some uses:

- **Follow a thread from your current session.** You are fixing a bug that a teammate reported in a Lark thread. Ask the agent to watch `im.message.receive_v1` filtered to that thread. New replies, such as a fresh stack trace or "still broken on staging", reach the session that already has the code open.
- **Hear only about messages that need you.** Ask the agent to watch the chats you care about. The watch's `decisions` argument asks a model whether each message needs you, for example a direct question or a blocked release. The agent messages you about those and skips the rest. This needs `LARK_DECISIONS_MODEL`.
- **Build a running report.** Ask the agent to watch a channel, such as incident updates or customer feedback, and write a summary when the watch ends. A watch expires after one hour by default, so a longer report needs a new watch each hour.

The server only listens, so the agent replies or notifies with its own tools, such as `lark-cli`.

## Before you start

The server does not create the Lark app, its permissions, or its event subscriptions, so set those up in the Lark developer console first. It connects to Lark, not Feishu. The package is private and not published to npm.

The server has these limits:

- Delivery is live only. Events that arrive while no watch is open, or while the connection is reconnecting, are lost.
- [Lark sends each event to one WebSocket client](https://open.larksuite.com/document/ukTMukTMukTM/uYDNxYjL2QTM24iN0EjN/event-subscription-configure-/use-websocket) of an app. Any other consumer of the same app, such as `lark-cli event consume` or this server on another machine, splits the events with it.
- Each watch expires after one hour by default.

To install, give your agent this README and ask it to follow the setup section.

---

## Setup for agents

Follow these steps on the machine that runs the OpenCode service. Ask the user for anything you cannot find.

### Requirements

- Git, Bun 1.4.4, and OpenCode 2.3.0.
- A Lark app with event subscriptions set to WebSocket mode. Ask the user for its app ID, which matches `cli_` followed by 16 hex characters, and its app secret.

### Install

1. Clone the repository and install dependencies at its root:

   ```sh
   git clone https://github.com/LuckUVeryX/oc-plugins.git "$HOME/.local/share/lark-events-mcp"
   cd "$HOME/.local/share/lark-events-mcp"
   bun install --frozen-lockfile
   ```

2. Make `LARK_APP_ID`, `LARK_APP_SECRET`, and Bun's directory on `PATH` available to the OpenCode service. OpenCode reads its own environment, which may differ from the user's shell. Do not write the secret into the config file.

3. Merge this into `~/.config/opencode/opencode.jsonc` and keep the existing settings:

   ```jsonc
   {
     "experimental": { "mcp_events": true },
     "mcp": {
       "servers": {
         "lark-events": {
           "type": "local",
           "command": [
             "bun",
             "{env:HOME}/.local/share/lark-events-mcp/packages/lark-events-mcp/src/main.ts",
           ],
           "cwd": "{env:HOME}/.local/share/lark-events-mcp",
           "pool": true,
           "protocol": "2026-07-28",
           "environment": {
             "LARK_APP_ID": "{env:LARK_APP_ID}",
             "LARK_APP_SECRET": "{env:LARK_APP_SECRET}",
           },
         },
       },
     },
   }
   ```

   If you cloned somewhere else, put the absolute path in `command` and `cwd`. OpenCode expands `{env:HOME}` but not `$HOME`.

   Put this block in the global config only. With `"pool": true`, projects in one OpenCode service share one Lark connection when the command, `cwd`, environment, protocol, and timeout match. A project-level override opens a second connection, and the two connections split the events.

4. Restart the OpenCode service so it reads the new environment and config.

### Optional settings

Add these to the `environment` block if the user wants them.

| Variable                 | Default | Effect                                                                                                                                                                                                                                                                                             |
| ------------------------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LARK_WATCH_MAX_SECONDS` | `3600`  | How long each watch lasts. Positive integer.                                                                                                                                                                                                                                                       |
| `LARK_DECISIONS_MODEL`   | unset   | Model that answers `decisions` questions on each event, as JSON: `{"providerID":"your-provider","id":"your-model","variant":"optional"}`. Omit `variant` if it is not needed. Without this variable, the server rejects watches that use `decisions`. Each matching event sends one model request. |

### Verify

1. Run `opencode mcp list`. `lark-events` should show `connected`. This checks the MCP connection only.
2. Ask the user which event the app subscribes to, open a watch on it with `watch_mcp_event`, and ask the user to trigger it. An `event` notification confirms delivery from Lark. Then call `stop_watch`.

### Troubleshoot

| Symptom                                   | Cause                                                                                                            |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Server fails to start                     | `LARK_APP_ID` is missing or malformed, or `LARK_APP_SECRET` is empty. Server logs go to stderr.                  |
| Watch stays open but never becomes active | The Lark connection cannot start, often because of a wrong secret. The watch waits until it expires.             |
| Some events arrive, others do not         | Another consumer of the same app is connected. Find it and stop it.                                              |
| An event never arrives                    | The app lacks the permission or subscription, or Lark does not deliver that event over WebSocket.                |
| Memory grows over time                    | Watches outlive the project that opened them and buffers are unbounded. Run `list_watches` and stop unused ones. |

### Update

Run this at the repository root, then restart the OpenCode service:

```sh
git pull --ff-only
bun install --frozen-lockfile
```
