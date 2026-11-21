import { Tool } from "@opencode/codemode";
import { Effect, Option, Record, Ref, Schema } from "effect";
import { type Decide, DecideInput } from "./decision-model.ts";
import { bytes, MAX_PREPARE_OUTPUT_BYTES, MAX_STATE_BYTES } from "./limits.ts";
import { Failed, makeScript } from "./script.ts";
import type { Integration, Tool as IntegrationTool } from "./integration.ts";
import { inputJsonSchema } from "./tool.ts";
import type { WatchEvent } from "./watch.ts";

export class StateTooLarge extends Schema.TaggedError<StateTooLarge>()("WatchState.TooLarge", {
  message: Schema.String,
}) {}

export interface WatchState {
  readonly get: Effect.Effect<Schema.Json>;
  readonly set: (value: Schema.Json) => Effect.Effect<Schema.Json, StateTooLarge>;
}

export const makeWatchState: Effect.Effect<WatchState> = Effect.gen(function* () {
  const value = yield* Ref.make<Schema.Json>(null);
  return {
    get: Ref.get(value),
    set: (next) =>
      Effect.gen(function* () {
        const encoded = JSON.stringify(next);
        if (bytes(encoded) > MAX_STATE_BYTES)
          return yield* Effect.fail(
            new StateTooLarge({
              message: `Watch state exceeds ${MAX_STATE_BYTES} bytes; keep the stored JSON smaller`,
            }),
          );
        yield* Ref.set(value, next);
        return next;
      }),
  };
});

// Code Mode renders the signature from JSON Schema and passes the raw value to run,
// so a provider decodes its input once whether the call comes from a script or MCP.
const codeModeTool = (tool: IntegrationTool) =>
  Tool.make({
    description: tool.description,
    input: inputJsonSchema(tool),
    output: Schema.Json,
    execute: (input) => tool.run(input),
  });

const stateGet = (state: WatchState) =>
  Tool.make({
    description:
      "Read this watch's own JSON memory, which starts as null. Use it to carry state between events, such as the newest note id you handled.",
    input: Schema.Struct({}),
    output: Schema.Json,
    execute: () => state.get,
  });

const stateSet = (state: WatchState) =>
  Tool.make({
    description: `Replace this watch's own JSON memory and return the new value. It holds up to ${MAX_STATE_BYTES / 1024} KB and lasts only as long as the watch.`,
    input: Schema.Json,
    output: Schema.Json,
    execute: (value) => state.set(value),
  });

const decideTool = (decide: Decide) =>
  Tool.make({
    description:
      "Judge a payload that a filter cannot settle, such as whether a message is a real request. Send text or JSON input and one or more named questions of type predicate, choice, or score. Check each answer's type before reading its probability. A question can return an error instead. One call is one model request.",
    input: DecideInput,
    output: Schema.Json,
    execute: (input) => decide(input),
  });

export const makePrepare = (options: {
  readonly integrations: ReadonlyArray<Integration>;
  readonly state: WatchState;
  readonly decide: Option.Option<Decide>;
}) => {
  const script = makeScript({
    tools: {
      watch: {
        state: {
          get: stateGet(options.state),
          set: stateSet(options.state),
        },
      },
      ...Object.fromEntries(
        options.integrations.map((integration) => [
          integration.name,
          Record.map(integration.tools, codeModeTool),
        ]),
      ),
      ...(Option.isNone(options.decide)
        ? {}
        : { events: { decide: decideTool(options.decide.value) } }),
    },
    limits: { maxOutputBytes: MAX_PREPARE_OUTPUT_BYTES },
  });
  return (code: string, event: WatchEvent): Effect.Effect<Schema.Json, Failed> =>
    script(code, { event });
};
