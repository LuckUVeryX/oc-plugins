import { Cause, Clock, DateTime, Deferred, Effect, Schema, type Scope, Stream } from "effect";
import { Health, type EventType, type Event, type SourceError } from "../source/contract.ts";
import { Failed, type RunCode } from "../code/code.ts";
import { Feeds } from "./feeds.ts";

// OpenCode ends an event stream after three of these pass without a notification.
export const HEARTBEAT_MS = 20_000;

export type Notification =
  | { readonly type: "active" | "heartbeat" }
  | {
      readonly type: "event";
      readonly eventId: string;
      readonly name: string;
      readonly timestamp: string;
      readonly data: Schema.Json;
    }
  | { readonly type: "error" | "terminated"; readonly reason: string };

class StepFailed extends Schema.TaggedError<StepFailed>()("Watch.StepFailed", {
  message: Schema.String,
}) {}

interface WatchRunOptions {
  readonly eventType: EventType;
  readonly params: Schema.JsonObject;
  readonly eventTimeoutSeconds: number;
  /** Ends the watch with the reason it succeeds with. */
  readonly until: Effect.Effect<string>;
  readonly runCode: RunCode;
  readonly notify: (notification: Notification) => Effect.Effect<void>;
}

const messageOf = (cause: Cause.Cause<unknown>): string => {
  const failure = Cause.squash(cause);
  return failure instanceof Failed ? failure.message : String(failure);
};

export const runWatch = Effect.fn("Watch.run")(function* (
  options: WatchRunOptions,
): Effect.fn.Return<string, SourceError, Feeds.Service | Scope.Scope> {
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
      const result = yield* options.runCode(delivered).pipe(
        Effect.timeout(`${options.eventTimeoutSeconds} seconds`),
        Effect.catchCause((cause) => {
          if (Cause.hasInterruptsOnly(cause)) return Effect.interrupt;
          return Effect.logWarning("Event step failed").pipe(
            Effect.annotateLogs({ event: options.eventType.name, eventId }),
            Effect.andThen(
              Effect.fail(
                new StepFailed({
                  message: `code failed on ${eventId}: ${messageOf(cause)}`,
                }),
              ),
            ),
          );
        }),
      );
      if (result === null) return;
      yield* options.notify({
        type: "event",
        eventId,
        name: options.eventType.name,
        timestamp: delivered.timestamp,
        data: result,
      });
    });
  const session = Effect.gen(function* () {
    const feed = yield* feeds.acquire({
      eventType: options.eventType,
      params: options.params,
    });
    const firstHealthAnnounced = yield* Deferred.make<void>();
    return yield* Effect.raceFirst(
      feed.events.pipe(
        Stream.runForEach((event) =>
          Deferred.await(firstHealthAnnounced).pipe(Effect.andThen(deliver(event))),
        ),
        Effect.as("Events stopped arriving. Arm a new watch"),
        Effect.catch((failure: SourceError | StepFailed) => Effect.succeed(failure.message)),
      ),
      feed.health.pipe(
        Stream.runForEach((value) =>
          announce(value).pipe(Effect.andThen(Deferred.succeed(firstHealthAnnounced, undefined))),
        ),
        Effect.andThen(Effect.never),
      ),
    );
  });
  const heartbeat = Effect.sleep(HEARTBEAT_MS).pipe(
    Effect.andThen(options.notify({ type: "heartbeat" })),
    Effect.forever,
  );
  const reason = yield* Effect.raceAllFirst([options.until, Effect.scoped(session), heartbeat]);
  yield* options.notify({ type: "terminated", reason });
  return reason;
});
