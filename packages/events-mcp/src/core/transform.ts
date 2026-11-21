import { Effect } from "effect";
import { MAX_EVENT_BYTES } from "./limits.ts";
import { Failed, makeScript } from "./script.ts";
import type { TransformInput } from "./watch.ts";

export const makeTransform = () => {
  const script = makeScript({
    limits: { maxToolCalls: 0, maxOutputBytes: MAX_EVENT_BYTES },
  });
  return (code: string, input: TransformInput): Effect.Effect<string | null, Failed> =>
    Effect.gen(function* () {
      const value = yield* script(code, input);
      if (value === null) return null;
      if (typeof value !== "string")
        return yield* Effect.fail(new Failed({ message: "InvalidOutput" }));
      return value;
    });
};
