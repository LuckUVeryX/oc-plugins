#!/usr/bin/env bun
import { serveStdio } from "@modelcontextprotocol/server/stdio";
import { BunRuntime } from "@effect/platform-bun";
import { Cause, ConfigProvider, Effect, Exit, Layer, Logger, Runtime } from "effect";
import { catalogLayer, LarkCatalog } from "./catalog.ts";
import { decisionModel, eventTimeoutSeconds, watchMaxSeconds } from "./config.ts";
import { makeDecisions } from "./decisions.ts";
import { EventProducers, producersLayer } from "./producers.ts";
import { makeServer } from "./server.ts";

const serve = (
  maxSeconds: number,
  eventTimeout: number,
  model: Effect.Success<typeof decisionModel>,
) =>
  Effect.gen(function* () {
    const catalog = yield* LarkCatalog;
    const producers = yield* EventProducers;
    yield* Effect.acquireRelease(
      Effect.sync(() =>
        serveStdio(
          () =>
            makeServer({
              catalog,
              producers,
              maxSeconds,
              eventTimeoutSeconds: eventTimeout,
              larkRequest: producers.larkRequest,
              decide: model._tag === "Some" ? makeDecisions(model.value) : undefined,
            }),
          {
            legacy: "reject",
            onerror: (error) => console.error(error),
          },
        ),
      ),
      (handle) => Effect.promise(() => handle.close()),
    );
    yield* Effect.callback<void>((resume) => {
      const onEnd = () => resume(Effect.void);
      process.stdin.once("end", onEnd);
      return Effect.sync(() => process.stdin.off("end", onEnd));
    });
  }).pipe(Effect.provide(Layer.mergeAll(catalogLayer, producersLayer)));

const program = Effect.gen(function* () {
  const maxSeconds = yield* watchMaxSeconds;
  const model = yield* decisionModel;
  const eventTimeout = yield* eventTimeoutSeconds;
  yield* serve(maxSeconds, eventTimeout, model);
}).pipe(
  Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true }))),
);

BunRuntime.runMain(program.pipe(Effect.scoped, Effect.provideService(Logger.LogToStderr, true)), {
  teardown: (exit, onExit) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? onExit(0)
      : Runtime.defaultTeardown(exit, onExit),
});
