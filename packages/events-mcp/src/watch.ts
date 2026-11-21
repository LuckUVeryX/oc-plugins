import { Cause, Clock, DateTime, Deferred, Effect, type Schema, type Scope, Stream } from "effect";
import { matchesFilter, type Filter } from "./contract.ts";
import { bytes, MAX_EVENT_BYTES } from "./limits.ts";
import { Health, type EventType, type Occurrence, type SourceError } from "./source.ts";
import type { Upstream } from "./upstream.ts";

// OpenCode cancels event subscriptions after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

export type WatchEvent = {
  readonly eventId: string;
  readonly timestamp: string;
  readonly data: Schema.JsonObject;
};

type PrepareRunner = (event: WatchEvent) => Effect.Effect<Schema.Json, string>;

export type TransformInput = {
  readonly event: WatchEvent;
  readonly prepared: Schema.Json;
};

type TransformRunner = (input: TransformInput) => Effect.Effect<string | null, string>;

export type NotificationType = "active" | "heartbeat" | "event" | "error" | "terminated";

interface WatchRunOptions {
  readonly watchId: string;
  readonly eventType: EventType;
  readonly params: Schema.JsonObject;
  readonly filter: Filter;
  readonly expiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
  readonly ended: Deferred.Deferred<string>;
  readonly upstream: Upstream;
  readonly prepare: PrepareRunner;
  readonly transform: TransformRunner;
  readonly notify: (type: NotificationType, body?: Record<string, unknown>) => Effect.Effect<void>;
}

const errorBody = (message: string) => ({ error: { code: -32000, message } });

/** A watch session ends on the occurrences stream's `SourceError` or on a prepare/transform message. */
type WatchFailure = SourceError | string;

export const runWatch = Effect.fn("Watch.run")(function* (
  options: WatchRunOptions,
): Effect.fn.Return<string, SourceError, Scope.Scope> {
  const announce = Health.$match({
    Live: () => options.notify("active"),
    Degraded: ({ reason }) => options.notify("error", errorBody(reason)),
  });
  const deliver = (occurrence: Occurrence) =>
    Effect.gen(function* () {
      if (!matchesFilter(options.filter, occurrence.payload)) return;
      const now = yield* Clock.currentTimeMillis;
      const occurredAt = occurrence.occurredAt ?? DateTime.makeUnsafe(now);
      const eventId = occurrence.id ?? `local-${crypto.randomUUID()}`;
      const event = {
        eventId,
        timestamp: DateTime.formatIso(occurredAt),
        data: occurrence.payload,
      };
      let step: "prepare" | "transform" = "prepare";
      const pipeline = Effect.gen(function* () {
        const prepared = yield* options.prepare(event);
        if (prepared === null) return null;
        step = "transform";
        return yield* options.transform({ event, prepared });
      });
      const result = yield* pipeline.pipe(
        Effect.timeout(`${options.eventTimeoutSeconds} seconds`),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
          return Effect.logWarning("Event step failed").pipe(
            Effect.annotateLogs({ watchId: options.watchId, eventId, step }),
            Effect.andThen(
              Effect.fail(`${step} failed on ${eventId}: ${String(Cause.squash(cause))}`),
            ),
          );
        }),
      );
      if (result === null) return;
      if (
        bytes(JSON.stringify({ id: eventId, timestamp: event.timestamp, data: result })) >
        MAX_EVENT_BYTES
      )
        return yield* Effect.fail(
          `transform failed on ${eventId}: the delivered event exceeds ${MAX_EVENT_BYTES} bytes; return a shorter string`,
        );
      yield* options.notify("event", {
        eventId,
        name: options.eventType.name,
        timestamp: event.timestamp,
        data: result,
      });
    });
  const session = Effect.gen(function* () {
    const subscription = yield* options.upstream.acquire({
      name: options.eventType.name,
      params: options.params,
    });
    const firstHealthAnnounced = yield* Deferred.make<void>();
    return yield* Effect.raceFirst(
      subscription.occurrences.pipe(
        Stream.runForEach((occurrence) =>
          Deferred.await(firstHealthAnnounced).pipe(Effect.andThen(deliver(occurrence))),
        ),
        Effect.as("Event source ended"),
        Effect.catch((failure: WatchFailure) =>
          Effect.succeed(typeof failure === "string" ? failure : failure.message),
        ),
      ),
      subscription.health.pipe(
        Stream.runForEach((value) =>
          announce(value).pipe(Effect.andThen(Deferred.succeed(firstHealthAnnounced, undefined))),
        ),
        Effect.andThen(Effect.never),
      ),
    );
  });
  const heartbeat = Effect.sleep(HEARTBEAT_INTERVAL).pipe(
    Effect.andThen(options.notify("heartbeat")),
    Effect.forever,
  );
  const reason = yield* Effect.raceAllFirst([
    Effect.sleep(`${options.expiresInSeconds} seconds`).pipe(Effect.as("Expired")),
    Deferred.await(options.ended),
    Effect.scoped(session),
    heartbeat,
  ]);
  yield* options.notify("terminated", errorBody(reason));
  return reason;
});
