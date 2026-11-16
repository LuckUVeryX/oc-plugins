import { Generate } from "@opencode/schema/generate";
import type { Session } from "@opencode/schema/session";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Queue,
  RcRef,
  Schema,
  Scope,
  Stream,
} from "effect";
import { Lark, LarkError } from "./lark.ts";
import { OpenCode, OpenCodeError, type Options } from "./opencode.ts";
import type { ConversationContext, LarkEvent, Root } from "./transform.ts";
import { type LoadedTransform, makeTransformLoader, TransformError } from "./transform-loader.ts";

export const WatchInput = Schema.Struct({
  questions: Generate.DecisionRequest.fields.questions,
  input: Generate.DecisionRequest.fields.input.pipe(Schema.withDecodingDefault(Effect.succeed([]))),
  transform: Schema.String.annotate({
    description:
      "JavaScript ES-module source text with a default-exported function whose resolved string queues a prompt and whose resolved null skips the batch. Use lark_transform_schema for the input and output shapes.",
  }),
  timeoutSeconds: Schema.Finite.check(Schema.isGreaterThan(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(7200)),
  ),
  debounceMilliseconds: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0))
    .pipe(Schema.withDecodingDefault(Effect.succeed(30_000)))
    .annotate({
      description:
        "Quiet period per conversation in milliseconds before processing a batch. Each new message resets that conversation's timer. Set 0 for no quiet-period wait. Independent of timeoutSeconds.",
    }),
});
export interface WatchInput extends Schema.Schema.Type<typeof WatchInput> {}

export const WatchStatus = Schema.Union([
  Schema.Struct({ state: Schema.Literal("stopped") }),
  Schema.Struct({
    state: Schema.Literal("active"),
    configuration: WatchInput,
    expiresAt: Schema.Finite,
    listener: Schema.Literal("listening"),
  }),
]);
export type WatchStatus = Schema.Schema.Type<typeof WatchStatus>;

export class WatchError extends Schema.TaggedError<WatchError>()("WatchError", {
  cause: Schema.Union([TransformError, LarkError, OpenCodeError]),
}) {}

export class Relay extends Context.Service<
  Relay,
  {
    readonly watch: (input: {
      readonly sessionID: Session.ID;
      readonly configuration: WatchInput;
    }) => Effect.Effect<WatchStatus, WatchError>;
    readonly stop: (input: { readonly sessionID: Session.ID }) => Effect.Effect<WatchStatus>;
    readonly status: (input: { readonly sessionID: Session.ID }) => Effect.Effect<WatchStatus>;
  }
>()("oc-lark-relay/Relay") {}

interface Source {
  readonly start: Effect.Effect<void>;
  closed: boolean;
}
interface Candidate {
  readonly sessionID: Session.ID;
  readonly configuration: WatchInput;
  readonly scope: Scope.Closeable;
  readonly events: Queue.Queue<LarkEvent>;
  readonly transform: LoadedTransform;
  readonly source: Source;
}
interface Subscription extends Candidate {
  readonly expiresAt: number;
}
type Command =
  | {
      readonly kind: "activate";
      readonly subscription: Candidate;
      readonly reply: Deferred.Deferred<WatchStatus, WatchError>;
    }
  | {
      readonly kind: "stop";
      readonly sessionID: Session.ID;
      readonly scope?: Scope.Closeable;
      readonly reply?: Deferred.Deferred<WatchStatus>;
    }
  | { readonly kind: "sourceClosed"; readonly source: Source }
  | {
      readonly kind: "status";
      readonly sessionID: Session.ID;
      readonly reply: Deferred.Deferred<WatchStatus>;
    };

const stopped: WatchStatus = { state: "stopped" };
const toStatus = (subscription: Subscription | undefined): WatchStatus =>
  subscription
    ? {
        state: "active",
        configuration: subscription.configuration,
        expiresAt: subscription.expiresAt,
        listener: "listening",
      }
    : stopped;

export const relayLayer = (options: Pick<Options, "workers">) =>
  Layer.effect(
    Relay,
    Effect.gen(function* () {
      const parent = yield* Scope.Scope;
      const lark = yield* Lark;
      const opencode = yield* OpenCode;
      const loadTransform = yield* makeTransformLoader;
      const commands = yield* Queue.unbounded<Command>();
      const registry = new Map<Session.ID, Subscription>();

      const close = (subscription: Subscription) =>
        Effect.gen(function* () {
          registry.delete(subscription.sessionID);
          yield* Scope.close(subscription.scope, Exit.void);
        });

      const sourceRef = yield* RcRef.make({
        acquire: Effect.gen(function* () {
          const stream = yield* lark.open();
          const scope = yield* Scope.Scope;
          const start = yield* stream.pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                for (const subscription of registry.values()) {
                  if (subscription.source === source) Queue.offerUnsafe(subscription.events, event);
                }
              }),
            ),
            Effect.onExit((exit) =>
              Effect.gen(function* () {
                if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)) return;
                source.closed = true;
                yield* Queue.offer(commands, { kind: "sourceClosed", source });
              }),
            ),
            Effect.catch((error) => Effect.logWarning("Lark source stopped", error)),
            Effect.forkIn(scope, { startImmediately: true }),
            Effect.asVoid,
            Effect.cached,
          );
          const source: Source = { start, closed: false };
          return source;
        }),
      });

      const start = Effect.fn("Relay.subscription")(function* (subscription: Subscription) {
        const scope = subscription.scope;
        const batches = yield* Queue.unbounded<{
          readonly root: Root;
          readonly events: readonly LarkEvent[];
        }>();
        const buffers = new Map<string, { events: LarkEvent[]; timer: Fiber.Fiber<void> }>();
        const processBatch = Effect.fn("Relay.batch")(function* (batch: {
          readonly root: Root;
          readonly events: readonly LarkEvent[];
        }) {
          const history = yield* lark.history(batch.root);
          const context: ConversationContext = { ...batch, history };
          const result = yield* opencode.decide({
            questions: subscription.configuration.questions,
            input: [...subscription.configuration.input, { type: "json", value: context }],
          });
          const text = yield* subscription.transform
            .run({
              ...context,
              result,
            })
            .pipe(
              Effect.catchTag("TransformError", (error) =>
                opencode
                  .queue({
                    sessionID: subscription.sessionID,
                    text: `Transform ${error.operation} error: ${error.message}\nThis batch was discarded. The watch remains active. Fix the transform and call lark_watch again.`,
                  })
                  .pipe(Effect.as(null)),
              ),
            );
          if (text !== null) yield* opencode.queue({ sessionID: subscription.sessionID, text });
        });
        for (let worker = 0; worker < options.workers; worker++) {
          yield* Queue.take(batches).pipe(
            Effect.flatMap((batch) =>
              processBatch(batch).pipe(
                Effect.catch((error) => Effect.logWarning("Lark batch discarded", error)),
              ),
            ),
            Effect.forever,
            Effect.forkIn(scope),
          );
        }
        yield* Stream.fromQueue(subscription.events).pipe(
          Stream.runForEach((event) =>
            Effect.gen(function* () {
              const root = {
                chatId: event.chat_id,
                rootId: event.root_id ?? event.message_id,
              };
              const key = JSON.stringify(root);
              const previous = buffers.get(key);
              const events = previous?.events ?? [];
              events.push(event);
              if (previous) yield* Fiber.interrupt(previous.timer);
              const timer = yield* Effect.sleep(
                subscription.configuration.debounceMilliseconds,
              ).pipe(
                Effect.flatMap(() => {
                  buffers.delete(key);
                  return Queue.offer(batches, { root, events });
                }),
                Effect.asVoid,
                Effect.forkIn(scope),
              );
              buffers.set(key, { events, timer });
            }),
          ),
          Effect.forkIn(scope, { startImmediately: true }),
        );
        yield* Effect.sleep(
          Math.max(0, subscription.expiresAt - (yield* Clock.currentTimeMillis)),
        ).pipe(
          Effect.andThen(
            Queue.offer(commands, {
              kind: "stop",
              sessionID: subscription.sessionID,
              scope: subscription.scope,
            }),
          ),
          Effect.forkIn(scope),
        );
      });

      yield* Queue.take(commands).pipe(
        Effect.flatMap((command) =>
          Effect.gen(function* () {
            switch (command.kind) {
              case "activate": {
                const candidate = command.subscription;
                const terminated = Scope.close(candidate.scope, Exit.void).pipe(
                  Effect.andThen(
                    Deferred.fail(
                      command.reply,
                      new WatchError({
                        cause: new LarkError({
                          operation: "activate",
                          cause: "Source terminated",
                        }),
                      }),
                    ),
                  ),
                );
                if (candidate.source.closed) return yield* terminated;
                const previous = registry.get(candidate.sessionID);
                if (previous) yield* close(previous);
                if (candidate.source.closed) return yield* terminated;
                const subscription: Subscription = {
                  ...candidate,
                  expiresAt:
                    (yield* Clock.currentTimeMillis) +
                    candidate.configuration.timeoutSeconds * 1000,
                };
                registry.set(subscription.sessionID, subscription);
                yield* start(subscription);
                yield* subscription.source.start;
                yield* Deferred.succeed(command.reply, toStatus(subscription));
                return;
              }
              case "stop": {
                const subscription = registry.get(command.sessionID);
                if (
                  subscription &&
                  (command.scope === undefined || subscription.scope === command.scope)
                ) {
                  yield* close(subscription);
                }
                if (command.reply) yield* Deferred.succeed(command.reply, stopped);
                return;
              }
              case "sourceClosed":
                for (const subscription of registry.values()) {
                  if (subscription.source === command.source) yield* close(subscription);
                }
                return;
              case "status": {
                yield* Deferred.succeed(command.reply, toStatus(registry.get(command.sessionID)));
              }
            }
          }),
        ),
        Effect.forever,
        Effect.forkScoped,
      );
      yield* opencode.deletedSessions.pipe(
        Stream.runForEach((sessionID) => Queue.offer(commands, { kind: "stop", sessionID })),
        Effect.catch((error) => Effect.logWarning("Session deletion listener stopped", error)),
        Effect.forkScoped,
      );

      return Relay.of({
        watch: Effect.fn("Relay.watch")(function* ({ sessionID, configuration }) {
          return yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const scope = yield* Scope.fork(parent);
              const subscription = yield* restore(
                Effect.gen(function* () {
                  const transform = yield* loadTransform({
                    source: configuration.transform,
                  }).pipe(Scope.provide(scope));
                  const source = yield* RcRef.get(sourceRef).pipe(Scope.provide(scope));
                  const events = yield* Queue.unbounded<LarkEvent>();
                  return {
                    sessionID,
                    configuration,
                    scope,
                    events,
                    transform,
                    source,
                  } satisfies Candidate;
                }),
              ).pipe(
                Effect.onExit((exit) =>
                  Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void,
                ),
              );
              const reply = yield* Deferred.make<WatchStatus, WatchError>();
              yield* Queue.offer(commands, {
                kind: "activate",
                subscription,
                reply,
              });
              return yield* restore(Deferred.await(reply));
            }),
          ).pipe(
            Effect.mapError((cause) =>
              cause instanceof WatchError ? cause : new WatchError({ cause }),
            ),
          );
        }),
        stop: Effect.fn("Relay.stop")(function* ({ sessionID }) {
          const reply = yield* Deferred.make<WatchStatus>();
          yield* Queue.offer(commands, { kind: "stop", sessionID, reply });
          return yield* Deferred.await(reply);
        }),
        status: Effect.fn("Relay.status")(function* ({ sessionID }) {
          const reply = yield* Deferred.make<WatchStatus>();
          yield* Queue.offer(commands, { kind: "status", sessionID, reply });
          return yield* Deferred.await(reply);
        }),
      });
    }),
  );
