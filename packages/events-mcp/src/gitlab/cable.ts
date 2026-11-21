import {
  Deferred,
  Duration,
  Effect,
  HashMap,
  Option,
  Queue,
  RcRef,
  Redacted,
  Ref,
  Schedule,
  Schema,
  Scope,
  Semaphore,
  Stream,
  SubscriptionRef,
} from "effect";
import { Health, SourceError } from "../source.ts";

export type Json = Schema.Json;

/** Action Cable identifies a subscription by a JSON object that names its channel. */
export interface CableIdentifier extends Schema.JsonObject {
  readonly channel: string;
}

export interface Cable {
  readonly subscribeUnique: (
    identifier: CableIdentifier,
  ) => Effect.Effect<Stream.Stream<Json, SourceError>, SourceError, Scope.Scope>;
  readonly health: Stream.Stream<Health>;
}

export interface CableOptions {
  readonly url: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly pingTimeout?: Duration.Input | undefined;
  readonly backoff?: Schedule.Schedule<unknown> | undefined;
}

export interface GitlabCableOptions {
  readonly host: string;
  readonly token: Redacted.Redacted<string>;
}

const ControlFrame = Schema.Union([
  Schema.Struct({ type: Schema.tag("welcome") }),
  Schema.Struct({ type: Schema.tag("ping"), message: Schema.Json }),
  Schema.Struct({ type: Schema.tag("confirm_subscription"), identifier: Schema.String }),
  Schema.Struct({
    type: Schema.tag("reject_subscription"),
    identifier: Schema.optionalKey(Schema.String),
  }),
  Schema.Struct({
    type: Schema.tag("disconnect"),
    reconnect: Schema.Boolean,
    reason: Schema.optionalKey(Schema.String),
  }),
]).pipe(Schema.toTaggedUnion("type"));

const MessageFrame = Schema.Struct({
  identifier: Schema.String,
  message: Schema.Json,
});

const InboundFrame = Schema.Union([ControlFrame, MessageFrame]);

const decodeInbound = Schema.decodeUnknownEffect(Schema.fromJsonString(InboundFrame));

type SocketEvent =
  | { readonly _tag: "Message"; readonly data: string }
  | { readonly _tag: "Close"; readonly code: number; readonly reason: string }
  | { readonly _tag: "Error"; readonly message: string };

const closeQuietly = (socket: WebSocket) => {
  try {
    socket.close(1000);
  } catch {}
};

const attach = (socket: WebSocket, events: Queue.Queue<SocketEvent>) => {
  socket.addEventListener("message", (event) => {
    Queue.offerUnsafe(events, { _tag: "Message", data: String(event.data) });
  });
  socket.addEventListener("close", (event) => {
    Queue.offerUnsafe(events, { _tag: "Close", code: event.code, reason: event.reason });
  });
  socket.addEventListener("error", () => {
    Queue.offerUnsafe(events, { _tag: "Error", message: "socket error" });
  });
};

type ConnectionState =
  | { readonly _tag: "Closed" }
  | { readonly _tag: "Open"; readonly socket: WebSocket }
  | { readonly _tag: "Failed"; readonly error: SourceError };

type SessionEnd =
  | { readonly _tag: "Disconnected"; readonly reason: string }
  | { readonly _tag: "Fatal"; readonly error: SourceError };

type SessionOutcome =
  | { readonly _tag: "Opened" }
  | { readonly _tag: "Unopened" }
  | { readonly _tag: "Fatal" };

type Welcome =
  | { readonly _tag: "Welcomed" }
  | { readonly _tag: "Unwelcomed" }
  | { readonly _tag: "Ended"; readonly end: SessionEnd };

interface Entry {
  readonly identifier: string;
  readonly messages: Queue.Queue<Json, SourceError>;
  readonly settled: Deferred.Deferred<void, SourceError>;
  readonly confirmed: Ref.Ref<boolean>;
}

const send = (socket: WebSocket, frame: unknown) =>
  Effect.try({
    try: () => socket.send(JSON.stringify(frame)),
    catch: () => SourceError.Unavailable({ message: "Cable socket send failed" }),
  });

const sendQuiet = (socket: WebSocket, frame: unknown) => send(socket, frame).pipe(Effect.ignore);

const frameDisconnect = (frame: {
  readonly reconnect: boolean;
  readonly reason?: string | undefined;
}): SessionEnd => {
  const reason = frame.reason ?? "disconnect";
  if (frame.reconnect) return { _tag: "Disconnected", reason };
  return {
    _tag: "Fatal",
    error:
      reason === "unauthorized"
        ? SourceError.Rejected({ message: "GitLab cable rejected the token" })
        : SourceError.Unavailable({ message: `GitLab cable disconnected: ${reason}` }),
  };
};

const defaultBackoff = Schedule.exponential("500 millis").pipe(
  Schedule.jittered,
  Schedule.modifyDelay(({ duration }) =>
    Effect.succeed(Duration.min(duration, Duration.seconds(30))),
  ),
);

export const makeCable: (options: CableOptions) => Effect.Effect<Cable, never, Scope.Scope> =
  Effect.fn("Cable.make")(function* (
    options: CableOptions,
  ): Effect.fn.Return<Cable, never, Scope.Scope> {
    const pingTimeout = options.pingTimeout ?? Duration.seconds(10);
    const backoff = options.backoff ?? defaultBackoff;
    const healthRef = yield* SubscriptionRef.make<Health>(
      Health.Degraded({ reason: "connecting" }),
    );

    const connection = yield* RcRef.make({
      acquire: Effect.gen(function* () {
        const connectionState = yield* Ref.make<ConnectionState>({ _tag: "Closed" });
        const subscriptions = yield* Ref.make(HashMap.empty<string, Entry>());
        const mutex = yield* Semaphore.make(1);
        yield* SubscriptionRef.set(healthRef, Health.Degraded({ reason: "connecting" }));
        const subscriptionSnapshot = Ref.get(subscriptions).pipe(
          Effect.map((live) => Array.from(HashMap.values(live))),
        );

        const publishLive = Effect.gen(function* () {
          const current = yield* Ref.get(connectionState);
          if (current._tag !== "Open") return;
          const entries = yield* subscriptionSnapshot;
          const confirmed = yield* Effect.forEach(entries, (entry) => Ref.get(entry.confirmed));
          if (confirmed.every((value) => value))
            yield* SubscriptionRef.set(healthRef, Health.Live());
        });

        const deliver = (identifier: string, message: Json) =>
          Effect.gen(function* () {
            const entry = HashMap.get(yield* Ref.get(subscriptions), identifier);
            if (Option.isNone(entry)) return;
            yield* Queue.offer(entry.value.messages, message);
          });

        const confirm = (identifier: string) =>
          mutex.withPermits(1)(
            Effect.gen(function* () {
              const entry = HashMap.get(yield* Ref.get(subscriptions), identifier);
              if (Option.isNone(entry)) return;
              yield* Ref.set(entry.value.confirmed, true);
              yield* publishLive;
              yield* Deferred.succeed(entry.value.settled, undefined);
            }),
          );

        const failEntries = (entries: ReadonlyArray<Entry>, error: SourceError) =>
          Effect.forEach(
            entries,
            (entry) =>
              Effect.all([Deferred.fail(entry.settled, error), Queue.fail(entry.messages, error)], {
                discard: true,
              }),
            { discard: true },
          );

        const reject = (identifier: Option.Option<string>) =>
          mutex.withPermits(1)(
            Effect.gen(function* () {
              const entries = yield* subscriptionSnapshot;
              const targets: Array<Entry> = [];
              for (const entry of entries) {
                if (Option.isSome(identifier)) {
                  if (entry.identifier === identifier.value) targets.push(entry);
                } else if (!(yield* Ref.get(entry.confirmed))) {
                  targets.push(entry);
                }
              }
              yield* failEntries(
                targets,
                SourceError.Rejected({ message: "GitLab cable rejected the subscription" }),
              );
            }),
          );

        const release = (identifier: string) =>
          mutex.withPermits(1)(
            Effect.gen(function* () {
              const entry = HashMap.get(yield* Ref.get(subscriptions), identifier);
              yield* Ref.update(subscriptions, HashMap.remove(identifier));
              if (Option.isSome(entry)) yield* Queue.shutdown(entry.value.messages);
              const current = yield* Ref.get(connectionState);
              if (current._tag === "Open")
                yield* sendQuiet(current.socket, { command: "unsubscribe", identifier });
            }),
          );

        const down = (end: SessionEnd) =>
          mutex.withPermits(1)(
            Effect.gen(function* () {
              if (end._tag === "Disconnected") {
                yield* Ref.set(connectionState, { _tag: "Closed" });
                return yield* SubscriptionRef.set(
                  healthRef,
                  Health.Degraded({ reason: end.reason }),
                );
              }
              yield* Ref.set(connectionState, { _tag: "Failed", error: end.error });
              yield* failEntries(yield* subscriptionSnapshot, end.error);
              yield* SubscriptionRef.set(healthRef, Health.Degraded({ reason: end.error.message }));
            }),
          );

        const handleFrame = (
          frame: Schema.Schema.Type<typeof InboundFrame>,
        ): Effect.Effect<SessionEnd | undefined> => {
          const keepGoing: Effect.Effect<SessionEnd | undefined> = Effect.succeed(undefined);
          const andKeepGoing = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            effect.pipe(Effect.andThen(keepGoing));
          if (!("type" in frame)) return andKeepGoing(deliver(frame.identifier, frame.message));
          return ControlFrame.match(frame, {
            welcome: () => keepGoing,
            ping: () => keepGoing,
            confirm_subscription: ({ identifier }) => andKeepGoing(confirm(identifier)),
            reject_subscription: ({ identifier }) =>
              andKeepGoing(reject(Option.fromUndefinedOr(identifier))),
            disconnect: (frame) => Effect.succeed(frameDisconnect(frame)),
          });
        };

        const awaitWelcome = (events: Queue.Queue<SocketEvent>): Effect.Effect<Welcome> =>
          Effect.gen(function* () {
            while (true) {
              const next = yield* Queue.take(events).pipe(Effect.timeoutOption(pingTimeout));
              if (Option.isNone(next) || next.value._tag !== "Message")
                return { _tag: "Unwelcomed" };
              const frame = yield* decodeInbound(next.value.data).pipe(Effect.option);
              if (Option.isNone(frame) || !("type" in frame.value)) continue;
              if (frame.value.type === "welcome") return { _tag: "Welcomed" };
              // A server can refuse the token before it welcomes the connection.
              if (frame.value.type === "disconnect")
                return { _tag: "Ended", end: frameDisconnect(frame.value) };
            }
          });

        const openSession = (socket: WebSocket) =>
          Effect.gen(function* () {
            yield* Ref.set(connectionState, { _tag: "Open", socket });
            const entries = yield* subscriptionSnapshot;
            yield* Effect.forEach(
              entries,
              (entry) =>
                Effect.gen(function* () {
                  yield* Ref.set(entry.confirmed, false);
                  yield* sendQuiet(socket, { command: "subscribe", identifier: entry.identifier });
                }),
              { discard: true },
            );
            yield* publishLive;
          });

        const drain = Effect.fn("Cable.drain")(function* (
          socket: WebSocket,
          events: Queue.Queue<SocketEvent>,
        ): Effect.fn.Return<SessionEnd> {
          while (true) {
            const next = yield* Queue.take(events).pipe(Effect.timeoutOption(pingTimeout));
            if (Option.isNone(next)) {
              closeQuietly(socket);
              return { _tag: "Disconnected", reason: "ping timeout" };
            }
            const event = next.value;
            switch (event._tag) {
              case "Close":
                return { _tag: "Disconnected", reason: `socket closed (${event.code})` };
              case "Error":
                closeQuietly(socket);
                return { _tag: "Disconnected", reason: event.message };
              case "Message": {
                const frame = yield* decodeInbound(event.data).pipe(Effect.option);
                if (Option.isNone(frame)) continue;
                const end = yield* handleFrame(frame.value);
                if (end !== undefined) {
                  closeQuietly(socket);
                  return end;
                }
                break;
              }
              default: {
                const _exhaustive: never = event;
                return _exhaustive;
              }
            }
          }
        });

        const session = Effect.fn("Cable.session")(function* (): Effect.fn.Return<
          SessionOutcome,
          never,
          Scope.Scope
        > {
          const events = yield* Queue.unbounded<SocketEvent>();
          const socket = yield* Effect.sync(
            () =>
              new WebSocket(options.url, {
                headers: { ...options.headers },
              }),
          );
          yield* Effect.sync(() => attach(socket, events));
          yield* Effect.addFinalizer(() => Effect.sync(() => closeQuietly(socket)));
          const welcome = yield* awaitWelcome(events);
          if (welcome._tag === "Unwelcomed") return { _tag: "Unopened" };
          if (welcome._tag === "Ended") {
            yield* down(welcome.end);
            return welcome.end._tag === "Fatal" ? { _tag: "Fatal" } : { _tag: "Unopened" };
          }
          yield* mutex.withPermits(1)(openSession(socket));
          const end = yield* drain(socket, events);
          yield* down(end);
          return end._tag === "Fatal" ? { _tag: "Fatal" } : { _tag: "Opened" };
        });

        const driver = Effect.fn("Cable.driver")(function* () {
          let step = yield* Schedule.toStepWithSleep(backoff);
          while (true) {
            const outcome = yield* Effect.scoped(session());
            if (outcome._tag === "Fatal") return;
            if (outcome._tag === "Opened") step = yield* Schedule.toStepWithSleep(backoff);
            yield* step(undefined);
          }
        });

        const acquireChannel = Effect.fn("Cable.acquireChannel")(function* (identifier: string) {
          const messages = yield* Queue.unbounded<Json, SourceError>();
          const settled = yield* Deferred.make<void, SourceError>();
          const confirmed = yield* Ref.make(false);
          const entry: Entry = { identifier, messages, settled, confirmed };
          yield* mutex.withPermits(1)(
            Effect.gen(function* () {
              const current = yield* Ref.get(connectionState);
              if (current._tag === "Failed") return yield* Effect.fail(current.error);
              yield* Ref.update(subscriptions, HashMap.set(identifier, entry));
              if (current._tag === "Open")
                yield* send(current.socket, { command: "subscribe", identifier }).pipe(
                  Effect.tapError(() => Ref.update(subscriptions, HashMap.remove(identifier))),
                );
            }),
          );
          yield* Effect.addFinalizer(() => release(identifier));
          yield* Deferred.await(settled);
          return Stream.fromQueue(messages);
        });

        yield* driver().pipe(Effect.forkScoped);
        return acquireChannel;
      }),
    });

    const subscribeUnique = Effect.fn("Cable.subscribeUnique")(function* (
      identifier: CableIdentifier,
    ): Effect.fn.Return<Stream.Stream<Json, SourceError>, SourceError, Scope.Scope> {
      const acquireChannel = yield* RcRef.get(connection);
      return yield* acquireChannel(JSON.stringify(identifier));
    });

    return {
      subscribeUnique,
      health: SubscriptionRef.changes(healthRef),
    };
  });

export const gitlabCable: (
  options: GitlabCableOptions,
) => Effect.Effect<Cable, never, Scope.Scope> = Effect.fn("Cable.gitlab")(function* (
  options: GitlabCableOptions,
): Effect.fn.Return<Cable, never, Scope.Scope> {
  return yield* makeCable({
    url: `wss://${options.host}/-/cable`,
    headers: {
      Authorization: `Bearer ${Redacted.value(options.token)}`,
      Origin: `https://${options.host}`,
    },
  });
});
