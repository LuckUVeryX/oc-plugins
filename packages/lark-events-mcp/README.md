# lark-events-mcp

This standalone stdio server exposes Lark SDK events through the experimental MCP Events extension. It uses `@larksuiteoapi/node-sdk` 1.76.0, Effect 4.1.0, and the official MCP server SDK 2.4.0.

The Lark app, its permissions, and its upstream event subscriptions must already be configured. The server does not manage them, send replies, or make decisions.

## Run

Use Bun 1.4.4. Set `LARK_APP_ID` and `LARK_APP_SECRET` in the launching environment, then run:

```sh
bun install --frozen-lockfile
bun packages/lark-events-mcp/src/main.ts
```

The server uses the Lark domain and app credentials. It reads no OpenCode configuration. SDK and Effect logs go to stderr. Stdout is reserved for MCP.

To install a local tarball:

```sh
cd packages/lark-events-mcp
bun pm pack --ignore-scripts --filename /tmp/opencode/lark-events-mcp-0.0.0.tgz
```

Install the tarball in a separate Bun project with `bun add /tmp/opencode/lark-events-mcp-0.0.0.tgz`. The executable is `lark-events-mcp`. The package is not a library API.

## OpenCode compatibility

A live test with stock OpenCode 2.3.0 verified that a filtered Lark message event wakes a session with `experimental.mcp_events` enabled and protocol `2026-07-28`. The binary's bundled client differs from the separately published `@modelcontextprotocol/client` 2.4.0.

Enable the experimental flag and select the modern protocol. Replace the checkout path:

```jsonc
{
  "experimental": { "mcp_events": true },
  "mcp": {
    "servers": {
      "lark-events": {
        "type": "local",
        "command": ["bun", "/absolute/path/to/oc-plugins/packages/lark-events-mcp/src/main.ts"],
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

The callback payload is the SDK's flattened value. Filter keys are scalar dot paths in that payload. Fields combine with AND. A nonempty array of expected values combines with OR. Comparisons use strict equality. Missing fields do not match. Unknown paths and values outside the generated type, enum, or const constraints are rejected. The server does not traverse arrays or interpret expressions. Omit `filter` to match all payloads.

Delivery is live-only. Cursors are null and resume requests are rejected. Notifications carry the opening request ID in `_meta["io.modelcontextprotocol/subscriptionId"]`. The server sends `active`, `event`, `heartbeat`, `error`, and `terminated` notifications as applicable. Heartbeats run every 20 seconds.

The server keeps one SDK connection per process. Subscribers have independent filters and cancellation. The SDK reconnects automatically after a disconnect. Events during the disconnected interval can be missed. A terminal SDK error ends existing subscriptions. The SDK can retry initial connection failures indefinitely.

SDK 1.76.0 treats an endpoint error response without a `data` field as retryable before it checks the error code. An invalid secret can produce this response and leave readiness pending without an error notification. Cancel the subscription to stop waiting. The server does not add a credential preflight or readiness deadline.

The server force-closes the SDK client when its scope ends. SDK cancellation is best effort. An in-flight endpoint request can finish after close, and a stalled WebSocket handshake has no default deadline.

Each event uses the SDK callback's `event_id` and millisecond `create_time` when available. Otherwise, the server assigns a `local-` UUID and local receipt time. Local IDs do not provide deduplication across restarts.
The event notification's `_meta["lark-events-mcp/timestampOrigin"]` is `upstream` or `local`.

Event buffers are in-memory and unbounded. Slow subscribers can increase memory use. There is no persistence or replay.

## Verification

```sh
bun run --filter lark-events-mcp check
bun run test:install
```

Regenerate the catalog with `bun run --filter lark-events-mcp generate`. `generate:check` verifies that the committed catalog matches the pinned SDK. The automated tests do not require Lark credentials.

The live stock OpenCode session test is separate from the automated package tests. It used a private bot chat, a local deterministic model provider, and no client patch. The test observed the watch registration, the synthetic session event, and a follow-up model request containing the event.
