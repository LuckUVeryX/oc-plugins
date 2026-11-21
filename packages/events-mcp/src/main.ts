#!/usr/bin/env bun
import { BunRuntime } from "@effect/platform-bun";
import { Cause, ConfigProvider, Effect, Exit, Layer, Logger, Runtime } from "effect";
import { Catalog } from "./core/catalog.ts";
import { Decisions } from "./core/decisions.ts";
import { Server } from "./core/server.ts";
import { Upstream } from "./core/upstream.ts";
import { makeGitlabSource } from "./gitlab/source.ts";
import { LarkApi } from "./lark/api.ts";
import { LarkConnection } from "./lark/connection.ts";
import { Lark } from "./lark/source.ts";

const LarkLayers = LarkConnection.layer.pipe(Layer.provideMerge(LarkApi.layer));
const CatalogLayer = Catalog.layer(Effect.all([Lark.source, makeGitlabSource()])).pipe(
  Layer.provide(LarkLayers),
);
const CoreLayer = Layer.mergeAll(Upstream.layer, Decisions.layer, CatalogLayer);
const AppLayer = Server.layer.pipe(Layer.provide(CoreLayer));

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
