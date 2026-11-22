#!/usr/bin/env bun
import { BunRuntime } from "@effect/platform-bun";
import { Cause, ConfigProvider, Effect, Exit, Layer, Logger, Runtime } from "effect";
import { FetchHttpClient } from "effect/http";
import { Registry } from "./core/registry.ts";
import { DecisionModel } from "./core/decision-model.ts";
import { Server } from "./core/server.ts";
import { Feeds } from "./core/feeds.ts";
import { isName, names } from "./harness/index.ts";
import { GitlabSocket } from "./gitlab/socket.ts";
import { GitlabClient } from "./gitlab/client.ts";
import { GitlabAuth } from "./gitlab/auth.ts";
import { Gitlab } from "./gitlab/integration.ts";
import { LarkAuth } from "./lark/auth.ts";
import { LarkSocket } from "./lark/socket.ts";
import { Lark } from "./lark/integration.ts";

const name = process.argv[2];
if (name === undefined || !isName(name)) {
  const shown = name === undefined ? "" : ` "${name}"`;
  console.error(`events-mcp: unknown harness${shown}. Use one of: ${names.join(", ")}.`);
  process.exit(1);
}

const LarkLayers = LarkSocket.layer.pipe(Layer.provideMerge(LarkAuth.layer));
const GitlabLayers = Layer.merge(GitlabSocket.layer, GitlabClient.layer).pipe(
  Layer.provideMerge(GitlabAuth.layer),
);
const IntegrationLayers = Layer.merge(LarkLayers, GitlabLayers).pipe(
  Layer.provide(FetchHttpClient.layer),
);
const RegistryLayer = Registry.layer(Effect.all([Lark.integration, Gitlab.integration])).pipe(
  Layer.provide(IntegrationLayers),
);
const CoreLayer = Layer.mergeAll(Feeds.layer, DecisionModel.layer, RegistryLayer);
const AppLayer = Server.layer(name).pipe(Layer.provide(CoreLayer));

const program = Effect.gen(function* () {
  const server = yield* Server.Service;
  yield* server.done;
}).pipe(
  Effect.provide(AppLayer),
  Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true }))),
  Effect.provideService(Logger.LogToStderr, true),
);

BunRuntime.runMain(program.pipe(Effect.scoped), {
  teardown: (exit, onExit) =>
    Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
      ? onExit(0)
      : Runtime.defaultTeardown(exit, onExit),
});
