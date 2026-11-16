# oc-lark-decisions

An OpenCode plugin that lets your agent watch Lark conversations and bring relevant messages into your current session. Use it to follow urgent requests or release blockers without checking Lark and copying messages into OpenCode yourself.

Add the plugin to `opencode.json` and choose a decision model from your OpenCode provider configuration:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "plugins": [
    {
      "package": "github:LuckUVeryX/oc-plugins#main::path:packages/oc-lark-decisions",
      "options": {
        "model": {
          "providerID": "your-provider",
          "id": "your-model",
        },
      },
    },
  ],
}
```

This GitHub target is available after the monorepo is published.
For a local checkout, use its `packages/oc-lark-decisions` directory instead.
The tested baseline is OpenCode 2.3.0 and Effect 4.1.0.
OpenCode needs access to `lark-cli` configured with bot access to received-message events and chat message history.

The package name is `oc-lark-decisions`. Its existing runtime plugin ID remains `oc-lark-relay`.
Use `oc-lark-decisions/transform` for transform type imports.

Then ask OpenCode to watch for messages:

```text
Start a Lark watch with lark_watch for production incidents, blocked releases,
or requests that need my attention within an hour. Notify me with the relevant
messages and a suggested next step.
```

The agent writes the transform and starts the watch. Describe which messages need attention and what you want the agent to do next. Matching messages queue prompts in this OpenCode session, not in Lark.

## Example: personal attention triage

The tools work directly with any decision policy, without a skill.
As an example, the [lark-attention skill](../../.agents/skills/lark-attention/SKILL.md)
helps an agent turn your responsibilities and current priorities into a watch
configuration. It separates relevance, unresolved work, the next actor, and
timing. Matching conversations return to one persistent session for investigation
and draft replies. It does not authorize automatic sending.

Copy the entire `lark-attention` directory into your session project's
`.agents/skills/` directory or your global `~/.config/opencode/skills/` directory.
Alternatively, add this checkout's absolute `.agents/skills` path to the
`skills` array in your OpenCode configuration. Installing the plugin alone does
not install the skill.

Then ask:

```text
Use lark-attention to draft my attention profile and watch configuration.
Show what would match and what would be ignored. Do not start the watch yet.
```

After reviewing the profile, ask the same session to start it for a chosen
duration. The decision model sees the profile supplied in `input`, not the
session's conversation. The skill includes reusable questions, a draft-only
transform, and authored classification examples. Tests verify their schema and
routing compatibility, not the model's classification accuracy.

## Watch lifetime

Each conversation waits for 30 seconds without a new message before evaluation. The watch expires after two hours. Ask for a different quiet period or duration to change these defaults. New messages reset the quiet period, not the expiry time.

The session can persist after the watch stops. Watches are in memory and do not
survive a plugin restart. Use `lark_status` to check listening state and expiry.

The default is 4 workers per active watch. To change this, set `options.workers` to a positive integer. The agent cannot change the configured model or worker count.

Starting a new watch replaces this session's previous watch and renews its duration. If registration fails, the previous watch and its expiry time stay unchanged.

If a transform fails during a batch, the plugin sends feedback to the agent so it can fix the transform and register it again. The watch stays active, but the failed batch is not retried.

## Limits

- The plugin watches received messages only and skips raw interactive-card messages.
- The decision policy selects relevant conversations. There is no chat or thread filter option.
- The plugin fetches the five most recent messages in the chat, not the full conversation.
- The plugin does not reconnect automatically after the Lark event source exits.
- Message content and recent chat history go to the decision model. Paid model requests can incur costs.
- Transforms run with the local plugin's privileges and are not sandboxed.
- Stopping or expiring a watch does not remove prompts already queued. Model requests already accepted can still incur costs.

## Develop

Use Bun 1.4.4. Run these commands from the repository root:

```sh
bun install --frozen-lockfile
bun run check
bun run test:install
```
