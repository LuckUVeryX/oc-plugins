import { CodeMode } from "@opencode/codemode";
import { Effect, Schema } from "effect";

export type TransformFailure =
  | CodeMode.DiagnosticKind
  | "InvalidInput"
  | "InputTooLarge"
  | "InvalidOutput"
  | "OutputTooLarge";

export const bytes = (value: string): number => Buffer.byteLength(value, "utf8");

export const makeTransform = () => {
  const runtime = CodeMode.make({
    limits: { timeoutMs: 1000, maxToolCalls: 0, maxOutputBytes: 16384 },
  });
  return (source: string, event: unknown): Effect.Effect<string | null, TransformFailure> =>
    Effect.gen(function* () {
      const encoded = yield* Effect.try(() => JSON.stringify({ event })).pipe(
        Effect.mapError((): TransformFailure => "InvalidInput"),
      );
      if (encoded === undefined || bytes(encoded) > 1048576)
        return yield* Effect.fail<TransformFailure>("InputTooLarge");
      const input = yield* Schema.decodeUnknownEffect(Schema.Json)(JSON.parse(encoded)).pipe(
        Effect.mapError((): TransformFailure => "InvalidInput"),
      );
      const result = yield* runtime.execute(source, { input });
      if (!result.ok) return yield* Effect.fail(result.error.kind);
      if (result.truncated) return yield* Effect.fail<TransformFailure>("Truncated");
      if (result.value === null) return null;
      if (typeof result.value !== "string")
        return yield* Effect.fail<TransformFailure>("InvalidOutput");
      const serialized = JSON.stringify(result.value);
      if (bytes(serialized) > 16384 || serialized.length > 16384)
        return yield* Effect.fail<TransformFailure>("OutputTooLarge");
      return result.value;
    });
};
