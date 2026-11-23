# oc-idle

An Effect v4 port of [vimtor's opencode-idle-compaction](https://github.com/vimtor/opencode-plugins/blob/main/packages/opencode-idle-compaction/plugin/src/server.ts).

Compacts OpenCode sessions after 54 idle minutes when their token estimate exceeds 200,000.
The tested baseline is OpenCode 2.4.0 and Effect 4.1.0.

## Install

```sh
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-idle'
```

This GitHub target is available after the monorepo is published.
For a local checkout, use its `packages/oc-idle` directory instead.
Configure options in `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": [
    {
      "package": "github:LuckUVeryX/oc-plugins#main::path:packages/oc-idle",
      "options": {
        "idleMinutes": 54,
        "minTokens": 200000,
      },
    },
  ],
}
```

Both options are optional and accept finite, non-negative numbers.
Set `idleMinutes` to `0` for no delay, or `minTokens` to `0` to qualify any positive estimate.
New activity cancels the session's timer. Child sessions are skipped.

## Develop

Use Bun 1.4.4. Run these commands from the repository root:

```sh
bun install --frozen-lockfile
bun run check
bun run test:install
```
