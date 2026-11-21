import { it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Ref, Stream } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vitest";
import { Health, type EventType } from "../../src/core/source.ts";
import { runWatch } from "../../src/core/watch.ts";

const tick: EventType = {
  name: "tick",
  description: "A tick.",
  params: {},
  payload: {},
  subscribe: () => Effect.die(new Error("The test upstream never subscribes")),
};

it.effect("keeps a watch for its chosen expiry", () =>
  Effect.gen(function* () {
    const health = Stream.succeed<Health>(Health.Live());
    const start = (expiresInSeconds: number) =>
      Effect.gen(function* () {
        const ended = yield* Deferred.make<string>();
        const seen = yield* Ref.make<ReadonlyArray<string>>([]);
        const fiber = yield* runWatch({
          watchId: "watch",
          eventType: tick,
          params: {},
          filter: [],
          expiresInSeconds,
          eventTimeoutSeconds: 30,
          ended,
          upstream: {
            acquire: () => Effect.succeed({ occurrences: Stream.never, health }),
          },
          prepare: () => Effect.succeed(null),
          transform: () => Effect.succeed(null),
          notify: (type) => Ref.update(seen, (all) => [...all, type]),
        }).pipe(Effect.forkScoped);
        return { fiber, seen };
      });

    const long = yield* start(28800);
    const standard = yield* start(3600);
    yield* TestClock.adjust("2 hours");
    expect(long.fiber.pollUnsafe()).toBeUndefined();
    expect(yield* Fiber.join(standard.fiber)).toBe("Expired");
    expect(long.fiber.pollUnsafe()).toBeUndefined();
    yield* TestClock.adjust("6 hours");
    expect(yield* Fiber.join(long.fiber)).toBe("Expired");
    const notable = (types: ReadonlyArray<string>) => types.filter((type) => type !== "heartbeat");
    expect(notable(yield* Ref.get(standard.seen))).toEqual(["active", "terminated"]);
    expect(notable(yield* Ref.get(long.seen))).toEqual(["active", "terminated"]);
  }),
);

it.effect("stop and expiry end a watch whose upstream never becomes ready", () =>
  Effect.gen(function* () {
    const start = (expiresInSeconds: number) =>
      Effect.gen(function* () {
        const ended = yield* Deferred.make<string>();
        const seen = yield* Ref.make<ReadonlyArray<string>>([]);
        const fiber = yield* runWatch({
          watchId: "never-ready",
          eventType: tick,
          params: {},
          filter: [],
          expiresInSeconds,
          eventTimeoutSeconds: 30,
          ended,
          upstream: { acquire: () => Effect.never },
          prepare: () => Effect.succeed(null),
          transform: () => Effect.succeed(null),
          notify: (type) => Ref.update(seen, (all) => [...all, type]),
        }).pipe(Effect.forkScoped);
        return { ended, seen, fiber };
      });

    const stopped = yield* start(3600);
    const expired = yield* start(60);
    yield* TestClock.adjust("30 seconds");
    expect(stopped.fiber.pollUnsafe()).toBeUndefined();
    yield* Deferred.succeed(stopped.ended, "Stopped");
    expect(yield* Fiber.join(stopped.fiber)).toBe("Stopped");
    expect(expired.fiber.pollUnsafe()).toBeUndefined();
    yield* TestClock.adjust("30 seconds");
    expect(yield* Fiber.join(expired.fiber)).toBe("Expired");
    const notable = (types: ReadonlyArray<string>) => types.filter((type) => type !== "heartbeat");
    expect(notable(yield* Ref.get(stopped.seen))).toEqual(["terminated"]);
    expect(notable(yield* Ref.get(expired.seen))).toEqual(["terminated"]);
  }),
);
