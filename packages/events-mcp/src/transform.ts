import { Effect } from "effect";
import { MAX_EVENT_BYTES } from "./limits.ts";
import { makeScript } from "./script.ts";
import type { TransformInput } from "./watch.ts";

export const makeTransform = () => {
  const script = makeScript({
    limits: { maxToolCalls: 0, maxOutputBytes: MAX_EVENT_BYTES },
  });
  return (source: string, input: TransformInput) =>
    Effect.gen(function* () {
      const value = yield* script(source, input);
      if (value === null) return null;
      if (typeof value !== "string") return yield* Effect.fail("InvalidOutput");
      return value;
    });
};
