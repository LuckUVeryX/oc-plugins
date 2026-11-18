import { CodeMode } from "@opencode/codemode";
import { Effect, Schema } from "effect";

export const bytes = (value: string): number => Buffer.byteLength(value, "utf8");

export const makeTransform = () => {
  const runtime = CodeMode.make({
    limits: { maxToolCalls: 0, maxOutputBytes: 16384 },
  });
  return (
    source: string,
    inputValue: { readonly event: unknown; readonly prepared?: unknown; readonly result?: unknown },
  ): Effect.Effect<string | null, string> =>
    Effect.gen(function* () {
      const encoded = yield* Effect.try(() => JSON.stringify(inputValue)).pipe(
        Effect.mapError(() => "InvalidInput"),
      );
      if (bytes(encoded) > 1048576) return yield* Effect.fail("InputTooLarge");
      const input = yield* Schema.decodeUnknownEffect(Schema.Json)(JSON.parse(encoded)).pipe(
        Effect.mapError(() => "InvalidInput"),
      );
      const result = yield* runtime.execute(source, { input });
      if (!result.ok) return yield* Effect.fail(`${result.error.kind}: ${result.error.message}`);
      if (result.truncated) return yield* Effect.fail("Truncated");
      if (result.value === null) return null;
      if (typeof result.value !== "string") return yield* Effect.fail("InvalidOutput");
      const serialized = JSON.stringify(result.value);
      if (bytes(serialized) > 16384) return yield* Effect.fail("OutputTooLarge");
      return result.value;
    });
};
