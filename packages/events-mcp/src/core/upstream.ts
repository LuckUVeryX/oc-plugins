import { Cache, Effect, RcMap, Ref, Schema, type Scope, Stream } from "effect";
import type { Catalog } from "./catalog.ts";
import type { Occurrence, SourceError, Subscription } from "./source.ts";

export interface SubscriptionKey {
  readonly name: string;
  readonly params: Schema.JsonObject;
}

export interface Upstream {
  readonly acquire: (key: SubscriptionKey) => Effect.Effect<Subscription, SourceError, Scope.Scope>;
}

export const makeUpstream = (catalog: Catalog): Effect.Effect<Upstream, never, Scope.Scope> =>
  Effect.gen(function* () {
    const subscriptions = yield* RcMap.make({
      lookup: (key: SubscriptionKey) =>
        Effect.gen(function* () {
          const event = catalog.get(key.name);
          if (event === undefined)
            return yield* Effect.die(new Error(`Unknown event: ${key.name}`));
          const claims = yield* Cache.make<string, Ref.Ref<boolean>>({
            capacity: 10_000,
            timeToLive: "1 hour",
            lookup: () => Ref.make(false),
          });
          const admit = (occurrence: Occurrence) =>
            Effect.gen(function* () {
              if (occurrence.id === undefined) return true;
              const claim = yield* Cache.get(claims, occurrence.id);
              return !(yield* Ref.getAndSet(claim, true));
            });
          const subscription = yield* event.subscribe(key.params);
          const occurrences = yield* subscription.occurrences.pipe(
            Stream.filterEffect(admit),
            Stream.share({ capacity: "unbounded" }),
          );
          return { occurrences, health: subscription.health };
        }),
      idleTimeToLive: 0,
    });
    const acquire: Upstream["acquire"] = Effect.fn("Upstream.acquire")(function* (key) {
      return yield* RcMap.get(subscriptions, key);
    });
    return { acquire };
  });
