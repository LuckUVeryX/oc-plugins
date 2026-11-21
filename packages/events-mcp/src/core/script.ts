import { CodeMode } from "@opencode/codemode";
import { Effect, Schema } from "effect";

export class Failed extends Schema.TaggedError<Failed>()("Script.Failed", {
  message: Schema.String,
}) {}

export type Script = (code: string, input: Schema.Json) => Effect.Effect<Schema.Json, Failed>;

export const makeScript = (options: CodeMode.Options): Script => {
  const runtime = CodeMode.make(options);
  return (code, input) =>
    Effect.gen(function* () {
      const result = yield* runtime.execute(code, { input });
      if (!result.ok) {
        return yield* Effect.fail(
          new Failed({ message: `${result.error.kind}: ${result.error.message}` }),
        );
      }
      if (result.truncated === true)
        return yield* Effect.fail(new Failed({ message: "Truncated" }));
      return result.value;
    });
};
