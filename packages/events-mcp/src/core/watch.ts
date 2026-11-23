import { Cause, Clock, DateTime, Deferred, Effect, Schema, type Scope, Stream } from "effect";
import { bytes, MAX_EVENT_BYTES } from "./limits.ts";
import { Health, type EventKind, type Event, type IntegrationError } from "./integration.ts";
import { Failed, type RunScript } from "./script.ts";
import { Feeds } from "./feeds.ts";

// OpenCode cancels event feeds after 60 seconds without a notification.
const HEARTBEAT_INTERVAL = "20 seconds";

export type Notification =
  | { readonly type: "active" | "heartbeat" }
  | {
      readonly type: "event";
      readonly eventId: string;
      readonly name: string;
      readonly timestamp: string;
      readonly data: string;
    }
  | { readonly type: "error" | "terminated"; readonly reason: string };

class StepFailed extends Schema.TaggedError<StepFailed>()("Watch.StepFailed", {
  message: Schema.String,
}) {}

interface WatchRunOptions {
  readonly watchId: string;
  readonly eventKind: EventKind;
  readonly params: Schema.JsonObject;
  readonly expiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
  readonly ended: Deferred.Deferred<string>;
  readonly script: RunScript;
  readonly notify: (notification: Notification) => Effect.Effect<void>;
  readonly onSubscribed?: Effect.Effect<void> | undefined;
}

const messageOf = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.squash(cause);
  return failure instanceof Failed ? failure.message : String(failure);
};

export const runWatch = Effect.fn("Watch.run")(function* (
  options: WatchRunOptions,
): Effect.fn.Return<string, IntegrationError, Feeds.Service | Scope.Scope> {
  const feeds = yield* Feeds.Service;
  const announce = Health.$match({
    Live: () => options.notify({ type: "active" }),
    Degraded: ({ reason }) => options.notify({ type: "error", reason }),
  });
  const deliver = (input: Event) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const occurredAt = input.occurredAt ?? DateTime.makeUnsafe(now);
      const eventId = input.id ?? `local-${crypto.randomUUID()}`;
      const delivered = {
        eventId,
        timestamp: DateTime.formatIso(occurredAt),
        data: input.payload,
      };
      const result = yield* options.script(delivered).pipe(
        Effect.timeout(`${options.eventTimeoutSeconds} seconds`),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
          return Effect.logWarning("Event step failed").pipe(
            Effect.annotateLogs({ watchId: options.watchId, eventId }),
            Effect.andThen(
              Effect.fail(
                new StepFailed({
                  message: `script failed on ${eventId}: ${messageOf(cause)}`,
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
            message: `script failed on ${eventId}: the delivered event exceeds ${MAX_EVENT_BYTES} bytes; return less`,
          }),
        );
      yield* options.notify({
        type: "event",
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
    if (options.onSubscribed) yield* options.onSubscribed;
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
    Effect.andThen(options.notify({ type: "heartbeat" })),
    Effect.forever,
  );
  const reason = yield* Effect.raceAllFirst([
    Effect.sleep(`${options.expiresInSeconds} seconds`).pipe(Effect.as("Expired")),
    Deferred.await(options.ended),
    Effect.scoped(session),
    heartbeat,
  ]);
  yield* options.notify({ type: "terminated", reason });
  return reason;
});
