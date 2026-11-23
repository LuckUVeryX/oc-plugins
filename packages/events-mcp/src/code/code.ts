import { CodeMode, Tool } from "@opencode/codemode";
import { Effect, Record, Ref, Schema } from "effect";
import { type Decide, DecideInput } from "../decision/decision-model.ts";
import { ToolError, type Source, type Tool as SourceTool } from "../source/contract.ts";
import { bytes, MAX_CODE_BYTES, MAX_EVENT_BYTES } from "../limits.ts";
import { inputJsonSchema, makeTool } from "../source/tool.ts";

export class Failed extends Schema.TaggedError<Failed>()("Code.Failed", {
  message: Schema.String,
}) {}

export type WatchEvent = {
  readonly eventId: string;
  readonly timestamp: string;
  readonly data: Schema.JsonObject;
};

/** Runs a watch's code on one event. null drops the event, and anything else is delivered. */
export type RunCode = (event: WatchEvent) => Effect.Effect<Schema.Json, Failed>;

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
        if (bytes(JSON.stringify(next)) > MAX_CODE_BYTES)
          return yield* Effect.fail(
            new StateTooLarge({
              message: `Watch state exceeds ${MAX_CODE_BYTES} bytes; keep the stored JSON smaller`,
            }),
          );
        yield* Ref.set(value, next);
        return next;
      }),
  };
});

// Code Mode renders the signature from JSON Schema and passes the raw value to run,
// so a provider decodes its input once whether the call comes from code or MCP.
const codeModeTool = (tool: SourceTool) =>
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
    description: `Replace this watch's own JSON memory and return the new value. It holds up to ${MAX_CODE_BYTES / 1024} KB and lasts only as long as the watch.`,
    input: Schema.Json,
    output: Schema.Json,
    execute: (value) => state.set(value),
  });

export const decideTool = (decide: Decide): SourceTool =>
  makeTool({
    description:
      "Judge a payload that plain code cannot settle, such as whether a message is a real request. Send text or JSON input and one or more named questions of type predicate, choice, or score. Check each answer's type before reading its probability. A question can return an error instead. One call is one model request.",
    input: DecideInput,
    run: (input) =>
      decide(input).pipe(Effect.mapError((error) => new ToolError({ message: error.message }))),
  });

export const makeRunCode = (options: {
  readonly code: string;
  readonly sources: ReadonlyArray<Source>;
  readonly state: WatchState;
  readonly decide: Decide;
}): RunCode => {
  const runtime = CodeMode.make({
    tools: {
      watch: { state: { get: stateGet(options.state), set: stateSet(options.state) } },
      ...Object.fromEntries(
        options.sources.map((source) => [source.name, Record.map(source.tools, codeModeTool)]),
      ),
      events: { decide: codeModeTool(decideTool(options.decide)) },
    },
    limits: { maxOutputBytes: MAX_CODE_BYTES },
  });
  return (event) =>
    Effect.gen(function* () {
      const result = yield* runtime.execute(options.code, { input: { event } });
      if (!result.ok)
        return yield* Effect.fail(
          new Failed({ message: `${result.error.kind}: ${result.error.message}` }),
        );
      if (result.truncated === true)
        return yield* Effect.fail(new Failed({ message: "Truncated" }));
      if (
        bytes(
          JSON.stringify({ id: event.eventId, timestamp: event.timestamp, data: result.value }),
        ) > MAX_EVENT_BYTES
      )
        return yield* Effect.fail(
          new Failed({
            message: `the delivered event exceeds ${MAX_EVENT_BYTES} bytes; return less`,
          }),
        );
      return result.value;
    });
};
