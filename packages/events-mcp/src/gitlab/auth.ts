import { Config, Context, Duration, Effect, Exit, Layer } from "effect";
import { HttpClient } from "effect/http";
import { type IntegrationError } from "../integration/contract.ts";
import { resolveIdentity, type GitlabIdentity } from "./token.ts";

export interface Interface {
  readonly host: string;
  readonly identity: Effect.Effect<GitlabIdentity, IntegrationError>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/GitlabAuth") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient;
    const host = yield* Config.String("GITLAB_HOST").pipe(
      Config.withDefault("gitlab.com"),
      Effect.orDie,
    );
    if (host.trim() === "") yield* Effect.die(new Error("GITLAB_HOST must not be empty"));
    const identity = yield* Effect.cachedWithTTL(
      resolveIdentity(host).pipe(Effect.provideService(HttpClient.HttpClient, http)),
      (exit) => (Exit.isSuccess(exit) ? Duration.infinity : Duration.zero),
    );
    return Service.of({ host, identity });
  }),
);

export * as GitlabAuth from "./auth.ts";
