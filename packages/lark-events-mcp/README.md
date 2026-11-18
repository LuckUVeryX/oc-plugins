# lark-events-mcp

This standalone stdio server exposes Lark SDK events through the experimental MCP Events extension. It is private and is not published to npm.

The Lark app, its permissions, and its upstream event subscriptions must already be configured. The server does not manage them, send replies, or make decisions.

## Install for OpenCode

You need Git, Bun 1.4.4, OpenCode, and a configured Lark app. Make sure Bun is on the OpenCode service's `PATH`.

1. Clone the repository into a dedicated directory and install dependencies at the repository root:

   ```sh
   git clone https://github.com/LuckUVeryX/oc-plugins.git "$HOME/.local/share/lark-events-mcp"
   cd "$HOME/.local/share/lark-events-mcp"
   bun install --frozen-lockfile
   ```

2. Set `LARK_APP_ID` and `LARK_APP_SECRET` in the OpenCode service's environment. OpenCode substitutes these values from its environment, not necessarily from your current shell.

3. Add the following settings to `~/.config/opencode/opencode.jsonc`, preserving any existing settings:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
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

4. Run `opencode mcp list`. The `lark-events` server should show `connected`. This confirms the MCP connection, not delivery of Lark events.

OpenCode expands `{env:HOME}` in the command and `cwd`. A literal `$HOME` in JSON is not expanded. If you chose another checkout directory, use its absolute path in both fields. The server uses the Lark domain and reserves stdout for MCP.

[Lark distributes each event to one WebSocket client](https://open.larksuite.com/document/ukTMukTMukTM/uYDNxYjL2QTM24iN0EjN/event-subscription-configure-/use-websocket) for an app, rather than broadcasting it. The [OpenCode connection pool](https://opencode.ai/docs/mcp-servers#shared-connections) shares one MCP connection across projects in one OpenCode service when their command, fixed `cwd`, environment, protocol, and timeout settings match. Use the global configuration above without project overrides for a single WebSocket connection. The pool does not span OpenCode services or machines, and closes when the last project releases the connection. Other CLI event consumers or SDK clients for the same app compete for events.

To update, run `git pull --ff-only` and `bun install --frozen-lockfile` at the repository root.

The server supports stock OpenCode 2.3.0 with `experimental.mcp_events` and protocol `2026-07-28`.

## MCP events

Use protocol `2026-07-28` and the `io.modelcontextprotocol/events` extension. `events/list` advertises SDK-known handler names and generated callback schemas. A catalog entry does not mean that the app has permission to receive it or that Lark delivers it over WebSocket.

Use OpenCode's `watch_mcp_event` with the `lark-events` server for native MCP subscriptions. The Lark CLI event consumer is a separate tool. Select an event in each `events/stream` request:

```json
{
  "name": "im.message.receive_v1",
  "arguments": {
    "watchId": "my-watch",
    "filter": {
      "message.chat_id": "oc_example"
    }
  },
  "cursor": null
}
```

Each active watch needs a nonempty ID unique within the server process. The operator can set `LARK_WATCH_MAX_SECONDS` in the server environment to a positive integer. It defaults to 3600 seconds. The lifetime starts when the watch is accepted, including connection readiness. Agents cannot override it. Use `watch_list` to see active IDs and their start and expiry times. Use `watch_stop` with a `watchId` to stop a watch and wait for cleanup. Expired and stopped watches send a terminal notification. Watches are not persisted across server restarts.

If you want to stop a watch, call `watch_stop` before disconnecting or closing the UI. A watch can outlive a project's disconnection.

Filter keys are exact scalar dot paths in the SDK's flattened callback payload. Fields combine with AND. A nonempty array of expected values combines with OR. Values use strict equality. Missing fields do not match. Unknown paths and values outside the generated type, enum, or const constraints are rejected. The server does not traverse arrays or interpret expressions. Omit `filter` to match all payloads.

Delivery is live-only. Cursors are null and resume requests are rejected. Notifications carry the opening request ID in `_meta["io.modelcontextprotocol/subscriptionId"]`. The server sends `active`, `event`, `heartbeat`, `error`, and `terminated` notifications as applicable. Heartbeats run every 20 seconds.

The SDK reconnects automatically, but events during a disconnect can be missed. A terminal SDK error ends existing subscriptions. Initial connection failures, including an invalid secret, can retry until the watch expires. Cancel or stop the subscription to stop waiting. SDK cancellation is best effort.

Each matching event produces one notification. Its `data` is the raw SDK callback payload. The notification keeps the upstream `event_id` and v2 millisecond `create_time` when valid. Otherwise it uses a `local-` UUID or local receipt time. `_meta["lark-events-mcp/timestampOrigin"]` identifies the timestamp as `upstream` or `local`. Local IDs do not provide deduplication across restarts.

This changes the previous batch notification contract. The `data.events` wrapper and the `debounce` argument are no longer supported. An OpenCode watch configured to run once receives one event. Oversized events may be truncated by OpenCode. Stopping or cancelling a watch discards pending events.

Only v2 event envelopes are supported. Names such as `im.message.receive_v1` still use v2 envelopes.

The server drops events with a v2 event-header timestamp more than one hour old before deduplication and delivery. Events without a valid header timestamp use local receipt time and are not age-filtered.

The server suppresses repeated upstream event IDs for the same event name. It keeps up to 10,000 IDs for one hour in memory. Events without an upstream ID are not deduplicated. Restarting the server clears the IDs.

Event buffers are in-memory and unbounded. Slow subscribers can increase memory use. There is no persistence or replay.
