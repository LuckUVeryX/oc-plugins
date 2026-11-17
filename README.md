# oc-plugins

Three OpenCode plugins and a standalone Lark Events MCP server in one Bun workspace.

| Package                                                   | Purpose                                                  |
| --------------------------------------------------------- | -------------------------------------------------------- |
| [oc-fallback](packages/oc-fallback/README.md)             | Switch models when a primary request hits the free limit |
| [oc-idle](packages/oc-idle/README.md)                     | Compact large sessions after an idle period              |
| [oc-lark-decisions](packages/oc-lark-decisions/README.md) | Evaluate Lark conversations and queue relevant prompts   |
| [lark-events-mcp](packages/lark-events-mcp/README.md)     | Expose Lark SDK events over MCP stdio                    |

Tested with OpenCode 2.3.0 and Effect 4.1.0. No build step is required.

## Install

Install the plugins you want:

```sh
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-fallback'
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-idle'
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-lark-decisions'
```

Replace `main` with a release tag to pin a version.
See each package's README for its options.

For a local checkout, run `bun install --frozen-lockfile` at the repository root.
Then add a package directory to your OpenCode config:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": ["/absolute/path/to/oc-plugins/packages/oc-idle"],
}
```

For Lark transforms, replace `oc-lark-relay/transform` imports with `oc-lark-decisions/transform`.
The runtime plugin ID is `oc-lark-decisions`, matching the package path.

### Standalone Lark Events MCP server

`lark-events-mcp` uses Effect and the official MCP server SDK. It has no OpenCode runtime dependency and does not evaluate models, route prompts, or send replies.
The package is private at version `0.0.0`, not a published npm package.

From a repository checkout:

```sh
bun install --frozen-lockfile
bun packages/lark-events-mcp/src/main.ts
```

An MCP client starts that command and owns its stdin. Supply `LARK_APP_ID` and `LARK_APP_SECRET` in its environment.
An offline test with stock OpenCode 2.3.0 verified event streaming and session wakeup with `experimental.mcp_events` enabled and protocol `2026-07-28`.
See the [server reference](packages/lark-events-mcp/README.md) for the client requirements, local tarball installation, event schemas, and lifecycle limits.

## Develop

Use Bun 1.4.4:

```sh
bun install --frozen-lockfile
bun run check
bun run test:install
```

`check` runs version checks, Oxfmt, Oxlint, TypeScript, and tests.
`test:install` tests packed plugins and the standalone server in fresh installations, separately and together.
GitHub Actions runs both.

Run the standalone server's verification independently:

```sh
bun run --filter lark-events-mcp check
```

The server tests use offline SDK and event-source fakes, not real Lark subscriptions or message sends.
The stock OpenCode session test is a separate offline experiment, not a package test. It uses no MCP client patch.

Use `bun run test`, not `bun test`, at the repository root.
Fallback uses Bun's runner. The other packages use Vitest.

To run one package's tests:

```sh
bun run --filter oc-idle test
```

To apply formatting:

```sh
bun run format
```

## Update dependencies

Edit dependency versions in the root `package.json`, then update the package manifests and lockfile:

```sh
bun run sync:metadata
bun install
bun run format
bun run check
bun run test:install
```

Keep explicit dependency versions in the package manifests so each package can install without the workspace.
`sync:metadata` copies versions from the root. CI checks that they match.

## Release

All packages share one version.
Change `version` in the root `package.json`, run the dependency-update commands above, then create one Git tag.

## License

This project is [MIT-licensed](LICENSE).
