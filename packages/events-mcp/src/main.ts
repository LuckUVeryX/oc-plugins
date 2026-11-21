#!/usr/bin/env bun
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { BunRuntime } from "@effect/platform-bun";
import { Cause, ConfigProvider, Effect, Exit, Logger, Runtime } from "effect";
import { makeCatalog } from "./catalog.ts";
import { decisionModel, stepTimeoutSeconds, watchDefaultSeconds } from "./config.ts";
import { makeDecisions } from "./decisions.ts";
import { makeGitlabSource } from "./gitlab/source.ts";
import { makeLarkSource } from "./lark/source.ts";
import { makeServer } from "./server.ts";
import { sdkLogger } from "./stderr-logger.ts";
import { makeUpstream } from "./upstream.ts";

const program = Effect.gen(function* () {
  const lark = yield* makeLarkSource();
  const gitlab = yield* makeGitlabSource();
  const catalog = yield* makeCatalog([lark, gitlab]);
  const upstream = yield* makeUpstream(catalog);
  const defaultExpiresInSeconds = yield* watchDefaultSeconds;
  const eventTimeoutSeconds = yield* stepTimeoutSeconds;
  const model = yield* decisionModel;
  yield* Effect.acquireRelease(
    Effect.sync(() =>
      serveStdio(
        () =>
          makeServer({
            catalog,
            upstream,
            defaultExpiresInSeconds,
            eventTimeoutSeconds,
            decide: model._tag === "Some" ? makeDecisions(model.value) : undefined,
          }),
        {
          legacy: "reject",
          onerror: (error) => sdkLogger.error(error),
        },
      ),
    ),
    (handle) => Effect.promise(() => handle.close()),
  );
  // The program's lifetime is the connection's: serve until the client closes stdin.
  yield* Effect.callback<void>((resume) => {
    const onEnd = () => resume(Effect.void);
    process.stdin.once("end", onEnd);
    return Effect.sync(() => process.stdin.off("end", onEnd));
  });
}).pipe(
  Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true }))),
  Effect.provideService(Logger.LogToStderr, true),
);

BunRuntime.runMain(program.pipe(Effect.scoped), {
  teardown: (exit, onExit) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? onExit(0)
      : Runtime.defaultTeardown(exit, onExit),
});
