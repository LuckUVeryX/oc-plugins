import { Cache, Context, Effect, Layer, RcMap, Ref, Schema, type Scope, Stream } from "effect";
import type { EventKind, Event, IntegrationError, Feed } from "../integration/contract.ts";

export interface FeedKey {
  readonly eventKind: EventKind;
  readonly params: Schema.JsonObject;
}

export interface Interface {
  readonly acquire: (key: FeedKey) => Effect.Effect<Feed, IntegrationError, Scope.Scope>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Feeds") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const feeds = yield* RcMap.make({
      lookup: (key: FeedKey) =>
        Effect.gen(function* () {
          const claims = yield* Cache.make<string, Ref.Ref<boolean>>({
            capacity: 10_000,
            timeToLive: "1 hour",
            lookup: () => Ref.make(false),
          });
          const admit = (event: Event) =>
            Effect.gen(function* () {
              if (event.id === undefined) return true;
              const claim = yield* Cache.get(claims, event.id);
              return !(yield* Ref.getAndSet(claim, true));
            });
          const feed = yield* key.eventKind.subscribe(key.params);
          const events = yield* feed.events.pipe(
            Stream.filterEffect(admit),
            Stream.share({ capacity: "unbounded" }),
          );
          return { events, health: feed.health };
        }),
      idleTimeToLive: 0,
    });
    const acquire = Effect.fn("Feeds.acquire")(function* (key: FeedKey) {
      return yield* RcMap.get(feeds, key);
    });
    return Service.of({ acquire });
  }),
);

export * as Feeds from "./feeds.ts";
