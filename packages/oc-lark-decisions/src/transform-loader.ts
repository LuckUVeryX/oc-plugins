import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { Host } from "@opencode/plugin/host";
import { Effect, FileSystem, Schema } from "effect";
import type { TransformInput } from "./transform.ts";

export class TransformError extends Schema.TaggedError<TransformError>()("TransformError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `${this.operation}: ${String(this.cause)}`;
  }
}

export interface LoadedTransform {
  readonly run: (input: TransformInput) => Effect.Effect<string | null, TransformError>;
}

const Module = Schema.Struct({
  default: Schema.declare(
    (value: unknown): value is (input: TransformInput) => unknown => typeof value === "function",
  ),
});
const Output = Schema.NullOr(Schema.String);

export const makeTransformLoader = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const dependencies = dirname(
    dirname(createRequire(import.meta.url).resolve("effect/package.json")),
  );
  const error = (operation: string) => (cause: unknown) =>
    cause instanceof TransformError ? cause : new TransformError({ operation, cause });

  return Effect.fn("loadTransform")(
    function* (input: { readonly source: string }) {
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "oc-lark-relay-" });
      yield* fs.symlink(dependencies, join(directory, "node_modules"));
      yield* fs.writeFileString(join(directory, "package.json"), '{"type":"module"}');
      const entry = join(directory, "source.ts");
      yield* fs.writeFileString(entry, input.source);
      const namespace = yield* Effect.tryPromise({
        try: () => Host.load(pathToFileURL(entry).href),
        catch: error("load"),
      });
      const module = yield* Schema.decodeUnknownEffect(Module)(namespace);
      return {
        run: Effect.fn("Transform.run")((value: TransformInput) =>
          Effect.tryPromise({
            try: () => Promise.resolve(module.default(value)),
            catch: error("run"),
          }).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(Output)),
            Effect.mapError(error("output")),
          ),
        ),
      } satisfies LoadedTransform;
    },
    (effect) => effect.pipe(Effect.mapError(error("load"))),
  );
});
