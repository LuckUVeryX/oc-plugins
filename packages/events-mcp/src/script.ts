import { CodeMode } from "@opencode/codemode";
import { Effect, type Schema } from "effect";

export type Script = (code: string, input: Schema.Json) => Effect.Effect<Schema.Json, string>;

export const makeScript = (options: CodeMode.Options): Script => {
  const runtime = CodeMode.make(options);
  return (code, input) =>
    Effect.gen(function* () {
      const result = yield* runtime.execute(code, { input });
      if (!result.ok) return yield* Effect.fail(`${result.error.kind}: ${result.error.message}`);
      if (result.truncated === true) return yield* Effect.fail("Truncated");
      return result.value;
    });
};
