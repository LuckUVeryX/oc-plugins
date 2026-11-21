import { Domain, EventDispatcher, WSClient } from "@larksuiteoapi/node-sdk";
import {
  Clock,
  DateTime,
  Deferred,
  Effect,
  Option,
  PubSub,
  RcMap,
  RcRef,
  Schema,
  type Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { Health, type Occurrence, SourceError, type Subscription } from "../core/source.ts";
import { sdkLogger } from "./sdk-logger.ts";
import { getLark, type LarkRequester } from "./get.ts";

const STALE_EVENT_CUTOFF_MS = 60 * 60 * 1000;

const unavailable = (message: string) => SourceError.Unavailable({ message });

const ConnectionCount = Schema.Struct({
  online_instance_cnt: Schema.optionalKey(Schema.Number),
});

const assertSoleConnection = (request: LarkRequester) =>
  Effect.gen(function* () {
    const data = yield* getLark(request, { path: "/event/v1/connection" }).pipe(
      Effect.mapError((message) =>
        unavailable(
          `Could not read the Lark event connection count, so the watch will not risk splitting events: ${message}`,
        ),
      ),
    );
    const count = Schema.decodeUnknownOption(ConnectionCount)(data);
    if (Option.isNone(count))
      return yield* Effect.fail(
        unavailable("Lark returned an unexpected response to the event connection check"),
      );
    const online = count.value.online_instance_cnt ?? 0;
    if (online > 0)
      return yield* Effect.fail(
        unavailable(
          `Another connection for this Lark app is online (${online}). Lark delivers each event to only one connection, so this watch would miss events. Stop the other consumer, such as lark-cli or this server on another machine, and arm a new watch.`,
        ),
      );
  });

const EventEnvelope = Schema.Struct({
  schema: Schema.optionalKey(Schema.String),
  event_id: Schema.optionalKey(Schema.String),
  create_time: Schema.optionalKey(Schema.String),
});

const toOccurrence = Effect.fn("Lark.toOccurrence")(function* (
  payload: unknown,
): Effect.fn.Return<Option.Option<Occurrence>> {
  const json = Schema.decodeUnknownOption(Schema.JsonObject)(payload);
  if (Option.isNone(json)) return Option.none();
  const envelope = Schema.decodeUnknownOption(EventEnvelope)(json.value);
  if (Option.isNone(envelope) || envelope.value.schema !== "2.0")
    return Option.some({ payload: json.value });
  const { event_id: id, create_time: time } = envelope.value;
  const occurredAt =
    time === undefined || !/^\d+$/.test(time) ? Option.none() : DateTime.make(Number(time));
  const staleBefore = (yield* Clock.currentTimeMillis) - STALE_EVENT_CUTOFF_MS;
  if (Option.isSome(occurredAt) && DateTime.toEpochMillis(occurredAt.value) < staleBefore)
    return Option.none();
  return Option.some({
    ...(id === undefined ? {} : { id }),
    ...(Option.isNone(occurredAt) ? {} : { occurredAt: occurredAt.value }),
    payload: json.value,
  });
});

const forward =
  (channel: PubSub.PubSub<Occurrence>) =>
  (payload: unknown): void => {
    Effect.runFork(
      Effect.gen(function* () {
        const occurrence = yield* toOccurrence(payload);
        if (Option.isSome(occurrence)) yield* PubSub.publish(channel, occurrence.value);
      }),
    );
  };

interface LarkCredentials {
  readonly appId: string;
  readonly appSecret: string;
  readonly request: LarkRequester;
}

export interface LarkConnection {
  readonly subscribe: (options: {
    readonly event: string;
  }) => Effect.Effect<Subscription, SourceError, Scope.Scope>;
}

export const makeConnection = (options: {
  readonly credentials: Effect.Effect<LarkCredentials, SourceError>;
}): Effect.Effect<LarkConnection, never, Scope.Scope> =>
  Effect.gen(function* () {
    const connection = yield* RcRef.make({
      acquire: Effect.gen(function* () {
        const credentials = yield* options.credentials;
        yield* assertSoleConnection(credentials.request);
        const health = yield* SubscriptionRef.make<Health>(Health.Live());
        const publishHealth = (next: Health) => Effect.runFork(SubscriptionRef.set(health, next));
        const terminal = yield* Deferred.make<never, SourceError>();
        const dispatcher = new EventDispatcher({ logger: sdkLogger });
        const channels = yield* RcMap.make({
          lookup: (event: string) =>
            Effect.gen(function* () {
              const channel = yield* Effect.acquireRelease(
                PubSub.unbounded<Occurrence>(),
                PubSub.shutdown,
              );
              dispatcher.register({ [event]: forward(channel) });
              return channel;
            }),
        });
        const ready = yield* Deferred.make<void, SourceError>();
        const abort = (error: SourceError) => {
          Effect.runFork(Deferred.fail(terminal, error));
          Effect.runFork(Deferred.fail(ready, error));
        };
        // Effect masks interruption during acquisition, so register the socket before waiting for readiness or an interrupted watch leaks it.
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            const client = new WSClient({
              appId: credentials.appId,
              appSecret: credentials.appSecret,
              domain: Domain.Lark,
              logger: sdkLogger,
              onReady: () => Effect.runFork(Deferred.succeed(ready, undefined)),
              onReconnecting: () =>
                publishHealth(
                  Health.Degraded({
                    reason: "Lark connection reconnecting; events may be missed",
                  }),
                ),
              onReconnected: () => publishHealth(Health.Live()),
              onError: (error) => abort(unavailable(error.message)),
            });
            void client
              .start({ eventDispatcher: dispatcher })
              .catch((error: unknown) =>
                abort(unavailable(error instanceof Error ? error.message : String(error))),
              );
            return client;
          }),
          (client) => Effect.sync(() => client.close({ force: true })),
        );
        yield* Deferred.await(ready);
        return { channels, health, terminal };
      }),
    });
    const subscribe: LarkConnection["subscribe"] = Effect.fn("LarkConnection.subscribe")(
      function* (request: { readonly event: string }) {
        const { channels, health, terminal } = yield* RcRef.get(connection);
        const channel = yield* RcMap.get(channels, request.event);
        const subscription = yield* PubSub.subscribe(channel);
        return {
          occurrences: Stream.fromSubscription(subscription).pipe(
            Stream.merge(Stream.fromEffect(Deferred.await(terminal))),
          ),
          health: SubscriptionRef.changes(health),
        };
      },
    );
    return { subscribe };
  });
