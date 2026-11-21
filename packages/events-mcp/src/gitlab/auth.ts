import { Config, Context, Duration, Effect, Exit, Layer } from "effect";
import { HttpClient } from "effect/http";
import { type IntegrationError } from "../core/integration.ts";
import { resolveIdentity, type GitlabIdentity } from "./token.ts";

export interface Interface {
  readonly host: string;
  readonly identity: Effect.Effect<GitlabIdentity, IntegrationError>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/GitlabAuth") {}

const cached = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Effect.Effect<A, E, R>, never, R> =>
  Effect.cachedWithTTL(effect, (exit) =>
    Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
  );

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const http = yield* HttpClient.HttpClient;
    const host = yield* Config.String("GITLAB_HOST").pipe(
      Config.withDefault("gitlab.example.com"),
      Effect.orDie,
    );
    if (host.trim() === "") yield* Effect.die(new Error("GITLAB_HOST must not be empty"));
    const identity = yield* cached(
      resolveIdentity(host).pipe(Effect.provideService(HttpClient.HttpClient, http)),
    );
    return Service.of({ host, identity });
  }),
);

export * as GitlabAuth from "./auth.ts";
