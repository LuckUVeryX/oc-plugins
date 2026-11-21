import { Cause, Clock, DateTime, Deferred, Effect, Schema, type Scope, Stream } from "effect";
import { matchesFilter, type Filter } from "./arguments.ts";
import { bytes, MAX_EVENT_BYTES } from "./limits.ts";
import { Health, type EventKind, type Event, type IntegrationError } from "./integration.ts";
import { Failed } from "./script.ts";
import { Feeds } from "./feeds.ts";

// OpenCode cancels event feeds after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

export type WatchEvent = {
  readonly eventId: string;
  readonly timestamp: string;
  readonly data: Schema.JsonObject;
};

type PrepareRunner = (event: WatchEvent) => Effect.Effect<Schema.Json, Failed>;

export type TransformInput = {
  readonly event: WatchEvent;
  readonly prepared: Schema.Json;
};

type TransformRunner = (input: TransformInput) => Effect.Effect<string | null, Failed>;

export type NotificationType = "active" | "heartbeat" | "event" | "error" | "terminated";

class StepFailed extends Schema.TaggedError<StepFailed>()("Watch.StepFailed", {
  message: Schema.String,
}) {}

interface WatchRunOptions {
  readonly watchId: string;
  readonly eventKind: EventKind;
  readonly params: Schema.JsonObject;
  readonly filter: Filter;
  readonly expiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
  readonly ended: Deferred.Deferred<string>;
  readonly prepare: PrepareRunner;
  readonly transform: TransformRunner;
  readonly notify: (type: NotificationType, body?: Record<string, unknown>) => Effect.Effect<void>;
}

const errorBody = (message: string) => ({ error: { code: -32000, message } });

const messageOf = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.squash(cause);
  return failure instanceof Failed ? failure.message : String(failure);
};

export const runWatch = Effect.fn("Watch.run")(function* (
  options: WatchRunOptions,
): Effect.fn.Return<string, IntegrationError, Feeds.Service | Scope.Scope> {
  const feeds = yield* Feeds.Service;
  const announce = Health.$match({
    Live: () => options.notify("active"),
    Degraded: ({ reason }) => options.notify("error", errorBody(reason)),
  });
  const deliver = (input: Event) =>
    Effect.gen(function* () {
      if (!matchesFilter(options.filter, input.payload)) return;
      const now = yield* Clock.currentTimeMillis;
      const occurredAt = input.occurredAt ?? DateTime.makeUnsafe(now);
      const eventId = input.id ?? `local-${crypto.randomUUID()}`;
      const delivered = {
        eventId,
        timestamp: DateTime.formatIso(occurredAt),
        data: input.payload,
      };
      let step: "prepare" | "transform" = "prepare";
      const pipeline = Effect.gen(function* () {
        const prepared = yield* options.prepare(delivered);
        if (prepared === null) return null;
        step = "transform";
        return yield* options.transform({ event: delivered, prepared });
      });
      const result = yield* pipeline.pipe(
        Effect.timeout(`${options.eventTimeoutSeconds} seconds`),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
          return Effect.logWarning("Event step failed").pipe(
            Effect.annotateLogs({ watchId: options.watchId, eventId, step }),
            Effect.andThen(
              Effect.fail(
                new StepFailed({
                  message: `${step} failed on ${eventId}: ${messageOf(cause)}`,
                }),
              ),
            ),
          );
        }),
      );
      if (result === null) return;
      if (
        bytes(JSON.stringify({ id: eventId, timestamp: delivered.timestamp, data: result })) >
        MAX_EVENT_BYTES
      )
        return yield* Effect.fail(
          new StepFailed({
            message: `transform failed on ${eventId}: the delivered event exceeds ${MAX_EVENT_BYTES} bytes; return a shorter string`,
          }),
        );
      yield* options.notify("event", {
        eventId,
        name: options.eventKind.name,
        timestamp: delivered.timestamp,
        data: result,
      });
    });
  const session = Effect.gen(function* () {
    const feed = yield* feeds.acquire({
      eventKind: options.eventKind,
      params: options.params,
    });
    const firstHealthAnnounced = yield* Deferred.make<void>();
    return yield* Effect.raceFirst(
      feed.events.pipe(
        Stream.runForEach((event) =>
          Deferred.await(firstHealthAnnounced).pipe(Effect.andThen(deliver(event))),
        ),
        Effect.as("Events stopped arriving. Arm a new watch"),
        Effect.catch((failure: IntegrationError | StepFailed) => Effect.succeed(failure.message)),
      ),
      feed.health.pipe(
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
