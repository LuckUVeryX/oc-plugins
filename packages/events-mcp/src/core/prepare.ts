import { Tool } from "@opencode/codemode";
import { Effect, Record, Ref, Schema } from "effect";
import { DecideInput } from "./decisions.ts";
import type { makeDecisions } from "./decisions.ts";
import { bytes, MAX_PREPARE_OUTPUT_BYTES, MAX_STATE_BYTES } from "./limits.ts";
import { makeScript } from "./script.ts";
import type { Source, Tool as SourceTool } from "./source.ts";
import { inputJsonSchema } from "./tool.ts";
import type { WatchEvent } from "./watch.ts";

export type DecisionsRequester = ReturnType<typeof makeDecisions>;

export interface WatchState {
  readonly get: Effect.Effect<Schema.Json>;
  readonly set: (value: Schema.Json) => Effect.Effect<Schema.Json, string>;
}

export const makeWatchState: Effect.Effect<WatchState> = Effect.gen(function* () {
  const value = yield* Ref.make<Schema.Json>(null);
  return {
    get: Ref.get(value),
    set: (next) =>
      Effect.gen(function* () {
        const encoded = JSON.stringify(next);
        if (encoded === undefined || bytes(encoded) > MAX_STATE_BYTES)
          return yield* Effect.fail(
            `Watch state exceeds ${MAX_STATE_BYTES} bytes; keep the stored JSON smaller`,
          );
        yield* Ref.set(value, next);
        return next;
      }),
  };
});

// Code Mode renders the signature from JSON Schema and passes the raw value to run,
// so a provider decodes its input once whether the call comes from a script or MCP.
const codeModeTool = (tool: SourceTool) =>
  Tool.make({
    description: tool.description,
    input: inputJsonSchema(tool),
    output: Schema.Json,
    execute: (input) => tool.run(input),
  });

const stateGet = (state: WatchState) =>
  Tool.make({
    description: "This watch's JSON memory; null until set.",
    input: Schema.Struct({}),
    output: Schema.Json,
    execute: () => state.get,
  });

const stateSet = (state: WatchState) =>
  Tool.make({
    description: "Replace this watch's JSON memory, up to 64 KB.",
    input: Schema.Json,
    output: Schema.Json,
    execute: (value) => state.set(value),
  });

const decideTool = (decide: DecisionsRequester) =>
  Tool.make({
    description:
      "Ask the decision model questions about input; returns an answer per question with probabilities. Use it to judge events a filter can't.",
    input: DecideInput,
    output: Schema.Json,
    execute: (input) => decide(input),
  });

export const makePrepare = (options: {
  readonly sources: ReadonlyArray<Source>;
  readonly state: WatchState;
  readonly decide?: DecisionsRequester | undefined;
}) => {
  const decide = options.decide;
  const script = makeScript({
    tools: {
      watch: {
        state: {
          get: stateGet(options.state),
          set: stateSet(options.state),
        },
      },
      ...Object.fromEntries(
        options.sources.map((source) => [source.name, Record.map(source.tools, codeModeTool)]),
      ),
      ...(decide === undefined ? {} : { events: { decide: decideTool(decide) } }),
    },
    limits: { maxOutputBytes: MAX_PREPARE_OUTPUT_BYTES },
  });
  return (source: string, event: WatchEvent): Effect.Effect<Schema.Json, string> =>
    script(source, { event });
};
