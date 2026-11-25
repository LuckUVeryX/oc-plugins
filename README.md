# oc-plugins

Two OpenCode plugins and a standalone events MCP server for Lark, GitLab, and Codemagic in one Bun workspace.

| Package                                       | Purpose                                                            |
| --------------------------------------------- | ------------------------------------------------------------------ |
| [oc-fallback](packages/oc-fallback/README.md) | Switch models when a primary request hits the free limit           |
| [oc-idle](packages/oc-idle/README.md)         | Compact large sessions after an idle period                        |
| [events-mcp](packages/events-mcp/README.md)   | Wake sessions on Lark, GitLab, and Codemagic events over MCP stdio |

Tested with OpenCode 2.4.1 and Effect 4.1.0. No build step is required.

## Install

Install the plugins you want:

```sh
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-fallback'
opencode plugin add 'github:LuckUVeryX/oc-plugins#main::path:packages/oc-idle'
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

For the standalone events MCP server, run:

```sh
bunx @luckuveryx/events-mcp@latest <client>
```

Use `opencode`, `claude`, or `codex` as the client. See the [events MCP installation and reference](packages/events-mcp/README.md) for client configuration, updating, versions, and release steps. `bunx` does not use this repository's `bun.lock`.

## Develop

Use Bun 1.4.4. Run `bun install --frozen-lockfile`, `bun run check`, and `bun run test:install` from the root. Use `bun run test`, not `bun test`, to run all tests.

For dependency updates, edit the root `package.json`, run `bun run sync:metadata`, `bun install`, and `bun run format`, then run both checks above. Package manifests need explicit versions for standalone installs. All packages share one version. For a release, update the root version, run the same commands, and push a `v<version>` tag. The publish workflow stages `@luckuveryx/events-mcp` on npm for approval.

## License

This project is [MIT-licensed](LICENSE).
