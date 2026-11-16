import { Context, Effect, FileSystem, Layer } from "effect";
import { Lark, larkLayer } from "../src/lark.ts";

export const controlledCLI = Effect.fn("Test.controlledCLI")(function* (body: string) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "relay-cli-test-" });
  const executable = `${directory}/lark-cli`;
  yield* fs.writeFileString(executable, `#!/bin/sh\n${body}\n`);
  yield* fs.chmod(executable, 0o755);
  const previous = process.env.PATH;
  yield* Effect.acquireRelease(
    Effect.sync(() => {
      process.env.PATH = `${directory}:${previous ?? ""}`;
    }),
    () =>
      Effect.sync(() => {
        if (previous === undefined) delete process.env.PATH;
        else process.env.PATH = previous;
      }),
  );
  const runtime = yield* Layer.build(larkLayer);
  return { lark: Context.get(runtime, Lark), directory, fs };
});
