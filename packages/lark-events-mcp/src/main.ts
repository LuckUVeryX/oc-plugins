#!/usr/bin/env bun
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { BunRuntime } from "@effect/platform-bun";
import { Cause, Effect, Exit, Layer, Logger, Runtime } from "effect";
import { catalogLayer, LarkCatalog } from "./catalog.ts";
import { EventProducers, producersLayer } from "./producers.ts";
import { makeServer } from "./server.ts";

const program = Effect.gen(function* () {
  const catalog = yield* LarkCatalog;
  const producers = yield* EventProducers;
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      serveStdio(() => makeServer({ catalog, producers }), {
        legacy: "reject",
        onerror: (error) => console.error(error),
      }),
    ),
    (handle) => Effect.promise(() => handle.close()),
  );
  yield* Effect.callback<void>((resume) => {
    const onEnd = () => resume(Effect.void);
    process.stdin.once("end", onEnd);
    return Effect.sync(() => process.stdin.off("end", onEnd));
  });
}).pipe(Effect.provide(Layer.mergeAll(catalogLayer, producersLayer)));

BunRuntime.runMain(program.pipe(Effect.scoped, Effect.provideService(Logger.LogToStderr, true)), {
  teardown: (exit, onExit) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? onExit(0)
      : Runtime.defaultTeardown(exit, onExit),
});
