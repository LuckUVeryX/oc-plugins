import { Cache, Context, Effect, Layer, RcMap, Ref, Schema, type Scope, Stream } from "effect";
import type { EventType, Occurrence, SourceError, Subscription } from "./source.ts";

export interface SubscriptionKey {
  readonly event: EventType;
  readonly params: Schema.JsonObject;
}

export interface Interface {
  readonly acquire: (key: SubscriptionKey) => Effect.Effect<Subscription, SourceError, Scope.Scope>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Upstream") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const subscriptions = yield* RcMap.make({
      lookup: (key: SubscriptionKey) =>
        Effect.gen(function* () {
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
          const subscription = yield* key.event.subscribe(key.params);
          const occurrences = yield* subscription.occurrences.pipe(
            Stream.filterEffect(admit),
            Stream.share({ capacity: "unbounded" }),
          );
          return { occurrences, health: subscription.health };
        }),
      idleTimeToLive: 0,
    });
    const acquire = Effect.fn("Upstream.acquire")(function* (key: SubscriptionKey) {
      return yield* RcMap.get(subscriptions, key);
    });
    return Service.of({ acquire });
  }),
);

export * as Upstream from "./upstream.ts";
