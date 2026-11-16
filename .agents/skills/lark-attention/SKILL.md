---
name: lark-attention
description: Configure personal Lark attention triage or tune its relevance profile and watch decisions.
---

# Lark attention

Author an attention profile and `lark_watch` configuration for this persistent
session. The tool descriptions own the event pipeline and lifecycle contract.

## 1. Establish what matters

Read [profile.md](profile.md). Draft it from the user's instructions and available
session context. Cite the source of each responsibility and priority. Mark
inferences and unknowns. Use confirmed Lark user IDs to distinguish the user from
the bot and namesakes.

Ask about gaps only when they change relevance. Unknown identity does not block
matching an explicitly confirmed project priority.

Keep the profile in this session unless the user requests a private file. Include
only facts needed for classification in the model input.

Default to read-only investigation and draft replies. Keep sending permission
separate from relevance. For requested automatic replies, establish allowed
categories and sending identity before deliberately changing the draft-only
transform. These instructions express policy, not runtime permission enforcement.

Complete this step with a dated profile containing at least one confirmed
responsibility, priority, or explicit request rule. Leave unsupported fields
unknown.

## 2. Turn the profile into decisions

Read [questions.json](questions.json), [transform.mjs](transform.mjs), and
[examples.md](examples.md). Keep the four decisions separate: relevance,
unresolved work, next actor, and timing. Customize instructions with the user's
domain examples. Preserve the names and values consumed by the transform.

Show examples using the user's actual priorities. Include an
unmentioned owned problem, a mention that requires nothing, a request someone
already answered, work owned by someone else, and missing context. Review the
expected classifications against the profile.

The template skips explicit nonmatches, completed work, and work owned by someone
else. Remaining unknown or failed classifications queue context review.

Complete this step when each example has an expected classification and handling.
These are authored expectations, not measured model accuracy.

## 3. Assemble and check the watch

Inspect `lark_status` and read `lark_transform_schema`. Preserve an existing watch
unless replacement was requested.

Construct the `lark_watch` arguments with:

- `questions`: the reviewed array from `questions.json`.
- `input`: a text part containing the dated profile, with expired priorities removed.
- `transform`: the source text of `transform.mjs`.
- Lifetime and quiet period: the user's requested values, or tool defaults.

Keep the profile and execution permissions in this session too. Standing `input`
goes to the decision model, not the transform.

For template changes in this checkout, run `bun run --filter oc-lark-decisions test`
from its root. For question changes, also evaluate representative conversations
with the configured model when available. [examples.md](examples.md#checking-changes)
defines the distinction between routing checks and classification evaluation.
Report unavailable checks as unrun.

Complete with the assembled configuration, check results, and unresolved profile
questions. For a configuration-only request, stop here.

## 4. Start and handle work

For a requested start or replacement, call `lark_watch` here, then `lark_status`.
Complete activation only when both confirm the requested configuration is active.
Report its expiry and coverage limits from the tool contract. Renew or restart
within the user's requested monitoring scope.

For queued work, retrieve the relevant conversation and recheck whether the
request remains unresolved. Apply this session's profile and permissions.
Investigate with permitted read-only tools. When the user must decide, present
the decision, evidence, and recommendation. Otherwise prepare a sourced answer.
Complete each review with source message identifiers, a draft reply, and any
remaining uncertainty.
