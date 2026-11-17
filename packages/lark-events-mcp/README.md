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

OpenCode expands `{env:HOME}` in the command. A literal `$HOME` in JSON is not expanded. If you chose another checkout directory, use its absolute path. The server uses the Lark domain and reserves stdout for MCP.

To update, run `git pull --ff-only` and `bun install --frozen-lockfile` at the repository root.

The server supports stock OpenCode 2.3.0 with `experimental.mcp_events` and protocol `2026-07-28`.

## MCP events

Use protocol `2026-07-28` and the `io.modelcontextprotocol/events` extension. `events/list` advertises SDK-known handler names and generated callback schemas. A catalog entry does not mean that the app has permission to receive it or that Lark delivers it over WebSocket.

Select an event in each `events/stream` request:

```json
{
  "name": "im.message.receive_v1",
  "arguments": {
    "filter": {
      "message.chat_id": "oc_example"
    }
  },
  "cursor": null
}
```

Filter keys are exact scalar dot paths in the SDK's flattened callback payload. Fields combine with AND. A nonempty array of expected values combines with OR. Values use strict equality. Missing fields do not match. Unknown paths and values outside the generated type, enum, or const constraints are rejected. The server does not traverse arrays or interpret expressions. Omit `filter` to match all payloads.

Delivery is live-only. Cursors are null and resume requests are rejected. Notifications carry the opening request ID in `_meta["io.modelcontextprotocol/subscriptionId"]`. The server sends `active`, `event`, `heartbeat`, `error`, and `terminated` notifications as applicable. Heartbeats run every 20 seconds.

The SDK reconnects automatically, but events during a disconnect can be missed. A terminal SDK error ends existing subscriptions. Initial connection failures, including an invalid secret, can retry indefinitely without an error notification. Cancel the subscription to stop waiting. SDK cancellation is best effort.

Each event uses the SDK callback's `event_id` and millisecond `create_time` when available. Otherwise, the server assigns a `local-` UUID and local receipt time. Local IDs do not provide deduplication across restarts.
The event notification's `_meta["lark-events-mcp/timestampOrigin"]` is `upstream` or `local`.

The server suppresses repeated upstream event IDs for the same event name. It keeps up to 10,000 IDs for one hour in memory. Events without an upstream ID are not deduplicated. Restarting the server clears the IDs.

Event buffers are in-memory and unbounded. Slow subscribers can increase memory use. There is no persistence or replay.
