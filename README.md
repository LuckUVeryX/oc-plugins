# oc-plugins

Three OpenCode plugins in one Bun workspace.

| Package                                                   | Purpose                                                  |
| --------------------------------------------------------- | -------------------------------------------------------- |
| [oc-fallback](packages/oc-fallback/README.md)             | Switch models when a primary request hits the free limit |
| [oc-idle](packages/oc-idle/README.md)                     | Compact large sessions after an idle period              |
| [oc-lark-decisions](packages/oc-lark-decisions/README.md) | Evaluate Lark conversations and queue relevant prompts   |

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

## Develop

Use Bun 1.4.4:

```sh
bun install --frozen-lockfile
bun run check
bun run test:install
```

`check` runs version checks, Oxfmt, Oxlint, TypeScript, and tests.
`test:install` tests packed plugins in fresh installations, separately and together.
GitHub Actions runs both.

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

## Licenses

`oc-fallback` is [MIT-licensed](packages/oc-fallback/LICENSE).
`oc-idle` and `oc-lark-decisions` have no declared license.
