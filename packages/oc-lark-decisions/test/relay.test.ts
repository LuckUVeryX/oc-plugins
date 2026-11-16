import { BunServices } from "@effect/platform-bun";
import { expect, it } from "@effect/vitest";
import type { Generate } from "@opencode/schema/generate";
import { Session } from "@opencode/schema/session";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Lark, LarkError } from "../src/lark.ts";
import { type DecisionInput, OpenCode, OpenCodeError, Options } from "../src/opencode.ts";
import { Relay, relayLayer, WatchInput } from "../src/relay.ts";
import type { ConversationContext, History, LarkEvent, Root } from "../src/transform.ts";

const sessionA = Session.ID.make("session-a");
const sessionB = Session.ID.make("session-b");
const event = (message = "message", root = "root"): LarkEvent => ({
  type: "im.message.receive_v1",
  chat_id: "chat",
  message_id: message,
  root_id: root,
  thread_id: "distinct-thread",
  content: `rendered:${message}`,
});
const configuration = (text = "literal", timeoutSeconds = 10): WatchInput =>
  Schema.decodeUnknownSync(WatchInput)({
    questions: [{ type: "predicate", name: text, instructions: "Is it urgent?" }],
    input: [{ type: "text", text: "additional evidence" }],
    transform: `export default () => ${JSON.stringify(text)}`,
    timeoutSeconds,
    debounceMilliseconds: 500,
  });

const fixtureWithWorkers = Effect.fn("Test.relay")(function* (workers: number) {
  const source = yield* Queue.unbounded<LarkEvent, LarkError>();
  let currentSource = source;
  const deleted = yield* Queue.unbounded<Session.ID>();
  const decisions = yield* Queue.unbounded<DecisionInput>();
  const delivered = yield* Queue.unbounded<{
    readonly sessionID: Session.ID;
    readonly text: string;
  }>();
  const roots = yield* Queue.unbounded<Root>();
  const cleanupStarted = yield* Queue.unbounded<void>();
  let cleanup: Effect.Effect<void> = Effect.void;
  let queueEffect: Effect.Effect<void, OpenCodeError> = Effect.void;
  let historyEffect: Effect.Effect<History, LarkError> = Effect.succeed({
    items: [{ message_id: "message", content: '{"text":"raw"}' }],
    hasMore: true,
  });
  let decisionEffect: Effect.Effect<Generate.DecisionResult, OpenCodeError> = Effect.succeed({
    answers: [
      { type: "predicate", name: "urgent", probability: 0.9 },
      { type: "error", name: "failed", reason: "individual failure" },
    ],
  });
  let opened = 0;
  let closed = 0;
  let initial: LarkEvent | undefined;
  let terminateOnOpen = false;
  const services = Layer.merge(
    Layer.succeed(
      Lark,
      Lark.of({
        open: () =>
          Effect.gen(function* () {
            opened++;
            yield* Effect.addFinalizer(() =>
              Effect.gen(function* () {
                yield* Queue.offer(cleanupStarted, undefined);
                yield* cleanup;
                closed++;
              }),
            );
            const stream = terminateOnOpen ? Stream.empty : Stream.fromQueue(currentSource);
            return initial ? Stream.concat(Stream.make(initial), stream) : stream;
          }),
        history: (root) =>
          Queue.offer(roots, root).pipe(Effect.andThen(Effect.suspend(() => historyEffect))),
      }),
    ),
    Layer.succeed(
      OpenCode,
      OpenCode.of({
        decide: (input) =>
          Queue.offer(decisions, input).pipe(Effect.andThen(Effect.suspend(() => decisionEffect))),
        queue: (input) =>
          Effect.suspend(() => queueEffect).pipe(
            Effect.andThen(Queue.offer(delivered, input)),
            Effect.asVoid,
          ),
        deletedSessions: Stream.fromQueue(deleted),
      }),
    ),
  );
  const scope = yield* Scope.fork(yield* Scope.Scope);
  const context = yield* Layer.build(
    relayLayer({ workers }).pipe(Layer.provide(services), Layer.provide(BunServices.layer)),
  ).pipe(Scope.provide(scope));
  return {
    relay: Context.get(context, Relay),
    source,
    deleted,
    decisions,
    delivered,
    roots,
    cleanupStarted,
    close: Scope.close(scope, Exit.void),
    opened: () => opened,
    closed: () => closed,
    setCleanup: (effect: Effect.Effect<void>) => {
      cleanup = effect;
    },
    setHistory: (effect: Effect.Effect<History, LarkError>) => {
      historyEffect = effect;
    },
    setDecision: (effect: Effect.Effect<Generate.DecisionResult, OpenCodeError>) => {
      decisionEffect = effect;
    },
    setQueue: (effect: Effect.Effect<void, OpenCodeError>) => {
      queueEffect = effect;
    },
    setInitial: (value: LarkEvent) => {
      initial = value;
    },
    setSource: (queue: Queue.Queue<LarkEvent, LarkError>) => {
      currentSource = queue;
    },
    terminateOnOpen: () => {
      terminateOnOpen = true;
    },
  };
});

const fixture = Schema.decodeUnknownEffect(Options)({
  model: { providerID: "operator-provider", id: "operator-model" },
}).pipe(Effect.flatMap(({ workers }) => fixtureWithWorkers(workers)));

it.effect(
  "fans out independently and preserves context, the full native result, and literal delivery",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const pass = configuration();
      const transform =
        "export default (input: import('oc-lark-decisions/transform').TransformInput) => JSON.stringify(input)";
      yield* f.relay.watch({ sessionID: sessionA, configuration: { ...pass, transform } });
      yield* f.relay.watch({ sessionID: sessionB, configuration: configuration("plain text") });
      expect(f.opened()).toBe(1);
      const incoming = event();
      yield* Queue.offer(f.source, incoming);
      yield* TestClock.adjust(0);
      expect(yield* Queue.size(f.source)).toBe(0);
      yield* TestClock.adjust("500 millis");
      expect(yield* Queue.size(f.decisions)).toBe(2);
      const first = yield* Queue.take(f.decisions);
      const second = yield* Queue.take(f.decisions);
      const expected: ConversationContext = {
        root: { chatId: "chat", rootId: "root" },
        events: [incoming],
        history: { items: [{ message_id: "message", content: '{"text":"raw"}' }], hasMore: true },
      };
      expect(first.input).toEqual([...pass.input, { type: "json", value: expected }]);
      expect(second.questions).not.toEqual(first.questions);
      const messages = [yield* Queue.take(f.delivered), yield* Queue.take(f.delivered)];
      expect(messages.find((message) => message.sessionID === sessionB)?.text).toBe("plain text");
      const transformed = messages.find((message) => message.sessionID === sessionA);
      expect(transformed && JSON.parse(transformed.text)).toEqual({
        ...expected,
        result: {
          answers: [
            { type: "predicate", name: "urgent", probability: 0.9 },
            { type: "error", name: "failed", reason: "individual failure" },
          ],
        },
      });
    }),
);

it.effect("a finite source stops its attached subscription without restarting", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.terminateOnOpen();
    const result = yield* f.relay
      .watch({
        sessionID: sessionA,
        configuration: configuration(),
      })
      .pipe(Effect.result);
    expect(result._tag).toBe("Success");
    yield* TestClock.adjust(0);
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
    expect(f.closed()).toBe(1);
    expect(f.opened()).toBe(1);
  }),
);

it.effect(
  "batches events within each conversation and falls back to the message ID, never the native thread ID",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
      const first = event("first");
      const second = event("second");
      const independent: LarkEvent = {
        type: "im.message.receive_v1",
        chat_id: "chat",
        message_id: "independent",
        thread_id: "not-the-root",
        content: "independent",
      };
      yield* Queue.offerAll(f.source, [first, second, independent]);
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      const calls = [yield* Queue.take(f.decisions), yield* Queue.take(f.decisions)];
      expect(calls.map((call) => call.input.at(-1))).toEqual([
        {
          type: "json",
          value: {
            root: { chatId: "chat", rootId: "root" },
            events: [first, second],
            history: {
              items: [{ message_id: "message", content: '{"text":"raw"}' }],
              hasMore: true,
            },
          },
        },
        {
          type: "json",
          value: {
            root: { chatId: "chat", rootId: "independent" },
            events: [independent],
            history: {
              items: [{ message_id: "message", content: '{"text":"raw"}' }],
              hasMore: true,
            },
          },
        },
      ]);
    }),
);

it.effect(
  "invalid replacement preserves its deadline; renewal cancels old buffers and keeps the source lease",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const before = yield* f.relay.watch({
        sessionID: sessionA,
        configuration: configuration("old"),
      });
      yield* Queue.offer(f.source, event("old"));
      yield* TestClock.adjust("100 millis");
      const invalid = yield* f.relay
        .watch({
          sessionID: sessionA,
          configuration: { ...configuration(), transform: "export default 42" },
        })
        .pipe(Effect.result);
      expect(invalid._tag).toBe("Failure");
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(before);
      const after = yield* f.relay.watch({
        sessionID: sessionA,
        configuration: configuration("new"),
      });
      expect(after).not.toEqual(before);
      expect(f.opened()).toBe(1);
      expect(f.closed()).toBe(0);
      yield* TestClock.adjust("500 millis");
      expect(yield* Queue.size(f.decisions)).toBe(0);
      yield* Queue.offer(f.source, event("new"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      expect((yield* Queue.take(f.delivered)).text).toBe("new");
      yield* f.relay.stop({ sessionID: sessionA });
      expect(f.closed()).toBe(1);
      expect(yield* f.relay.stop({ sessionID: sessionA })).toEqual({ state: "stopped" });
    }),
);

for (const source of [
  "export default ( => ;",
  'import "definitely-not-installed-relay-example"; export default () => null',
  "export const notDefault = () => null",
  'throw new Error("initialization threw"); export default () => null',
  'await Promise.reject(new Error("initialization rejected")); export default () => null',
]) {
  it.effect(
    `failed module loading preserves the active configuration and deadline: ${source}`,
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const before = yield* f.relay.watch({
          sessionID: sessionA,
          configuration: configuration("original"),
        });
        yield* TestClock.adjust("1 second");
        const replacement = yield* f.relay
          .watch({
            sessionID: sessionA,
            configuration: { ...configuration("replacement"), transform: source },
          })
          .pipe(Effect.result);
        expect(replacement._tag).toBe("Failure");
        expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(before);
        expect(f.opened()).toBe(1);
        expect(f.closed()).toBe(0);
        yield* Queue.offer(f.source, event());
        yield* TestClock.adjust(0);
        yield* TestClock.adjust("500 millis");
        expect((yield* Queue.take(f.delivered)).text).toBe("original");
      }),
  );
}

for (const action of ["stop", "expiry", "replacement"]) {
  it.effect(`${action} cancels a pending decisions await and prevents its later delivery`, () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const started = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const result = yield* Deferred.make<Generate.DecisionResult>();
      f.setDecision(
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(result)),
          Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
        ),
      );
      yield* f.relay.watch({
        sessionID: sessionA,
        configuration: configuration("old", action === "expiry" ? 1 : 10),
      });
      yield* Queue.offer(f.source, event());
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      yield* Deferred.await(started);
      if (action === "expiry") yield* TestClock.adjust("500 millis");
      else if (action === "stop") yield* f.relay.stop({ sessionID: sessionA });
      else
        yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("replacement") });
      yield* Deferred.await(interrupted);
      yield* Deferred.succeed(result, { answers: [] });
      yield* TestClock.adjust(0);
      expect(yield* Queue.size(f.delivered)).toBe(0);
      expect((yield* f.relay.status({ sessionID: sessionA })).state).toBe(
        action === "replacement" ? "active" : "stopped",
      );
    }),
  );

  it.effect(
    `${action} cancels transform waiting without claiming to terminate the user's promise`,
    () =>
      Effect.gen(function* () {
        const f = yield* fixture;
        const started = yield* Deferred.make<void>();
        const { promise, resolve } = Promise.withResolvers<string>();
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            Object.defineProperty(globalThis, "__relayTransformWait", {
              configurable: true,
              value: () => {
                Effect.runSync(Deferred.succeed(started, undefined));
                return promise;
              },
            });
          }),
          () => Effect.sync(() => Reflect.deleteProperty(globalThis, "__relayTransformWait")),
        );
        yield* f.relay.watch({
          sessionID: sessionA,
          configuration: {
            ...configuration("old", action === "expiry" ? 1 : 10),
            transform: 'export default () => Reflect.get(globalThis, "__relayTransformWait")();',
          },
        });
        yield* Queue.offer(f.source, event());
        yield* TestClock.adjust(0);
        yield* TestClock.adjust("500 millis");
        yield* Deferred.await(started);
        if (action === "expiry") yield* TestClock.adjust("500 millis");
        else if (action === "stop") yield* f.relay.stop({ sessionID: sessionA });
        else
          yield* f.relay.watch({
            sessionID: sessionA,
            configuration: configuration("replacement"),
          });
        yield* Effect.sync(() => resolve("late transform result"));
        expect(yield* Effect.promise(() => promise)).toBe("late transform result");
        yield* TestClock.adjust(0);
        expect(yield* Queue.size(f.delivered)).toBe(0);
        expect((yield* f.relay.status({ sessionID: sessionA })).state).toBe(
          action === "replacement" ? "active" : "stopped",
        );
      }),
  );
}

it.effect("events and reads do not renew expiry; deletion and unload cancel only owned work", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("a", 1) });
    yield* f.relay.watch({ sessionID: sessionB, configuration: configuration("b", 10) });
    yield* TestClock.adjust("750 millis");
    yield* Queue.offer(f.source, event());
    yield* f.relay.status({ sessionID: sessionA });
    yield* TestClock.adjust("250 millis");
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
    expect((yield* f.relay.status({ sessionID: sessionB })).state).toBe("active");
    yield* Queue.offer(f.deleted, sessionB);
    yield* TestClock.adjust(0);
    expect(yield* f.relay.status({ sessionID: sessionB })).toEqual({ state: "stopped" });
    yield* TestClock.adjust("1 second");
    expect(yield* Queue.size(f.delivered)).toBe(0);
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    yield* Queue.offer(f.source, event());
    yield* f.close;
    expect(f.closed()).toBe(2);
  }),
);

it.effect("unload awaits worker interruption and final source cleanup without late delivery", () =>
  Effect.gen(function* () {
    const f = yield* fixtureWithWorkers(1);
    const working = yield* Deferred.make<void>();
    const interrupting = yield* Deferred.make<void>();
    const releaseWorker = yield* Deferred.make<void>();
    const releaseSource = yield* Deferred.make<void>();
    const result = yield* Deferred.make<Generate.DecisionResult>();
    const unloaded = yield* Deferred.make<void>();
    f.setDecision(
      Deferred.succeed(working, undefined).pipe(
        Effect.andThen(Deferred.await(result)),
        Effect.onInterrupt(() =>
          Deferred.succeed(interrupting, undefined).pipe(
            Effect.andThen(Deferred.await(releaseWorker)),
          ),
        ),
      ),
    );
    f.setCleanup(Deferred.await(releaseSource));
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    yield* Queue.offer(f.source, event());
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    yield* Deferred.await(working);
    const close = yield* f.close.pipe(
      Effect.tap(() => Deferred.succeed(unloaded, undefined)),
      Effect.forkScoped,
    );
    yield* Deferred.await(interrupting);
    expect(yield* Deferred.isDone(unloaded)).toBe(false);
    expect(f.closed()).toBe(0);
    yield* Deferred.succeed(result, { answers: [] });
    yield* TestClock.adjust(0);
    expect(yield* Queue.size(f.delivered)).toBe(0);
    yield* Deferred.succeed(releaseWorker, undefined);
    yield* Queue.take(f.cleanupStarted);
    expect(yield* Deferred.isDone(unloaded)).toBe(false);
    expect(f.closed()).toBe(0);
    yield* Deferred.succeed(releaseSource, undefined);
    yield* Fiber.join(close);
    expect(yield* Deferred.isDone(unloaded)).toBe(true);
    yield* Queue.offer(f.source, event("late"));
    yield* TestClock.adjust("1 second");
    expect(yield* Queue.size(f.delivered)).toBe(0);
    expect(yield* Queue.size(f.decisions)).toBe(1);
    expect(f.opened()).toBe(1);
    expect(f.closed()).toBe(1);
    yield* f.close;
    expect(f.closed()).toBe(1);
  }),
);

it.effect("each session uses its own configured conversation quiet period", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({
      sessionID: sessionA,
      configuration: { ...configuration("fast"), debounceMilliseconds: 100 },
    });
    yield* f.relay.watch({
      sessionID: sessionB,
      configuration: { ...configuration("slow"), debounceMilliseconds: 700 },
    });
    yield* Queue.offer(f.source, event());
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("99 millis");
    expect(yield* Queue.size(f.decisions)).toBe(0);
    yield* TestClock.adjust("1 millis");
    expect(yield* Queue.take(f.delivered)).toEqual({ sessionID: sessionA, text: "fast" });
    expect((yield* Queue.take(f.decisions)).questions[0]?.name).toBe("fast");
    yield* TestClock.adjust("599 millis");
    expect(yield* Queue.size(f.decisions)).toBe(0);
    expect(yield* Queue.size(f.delivered)).toBe(0);
    yield* TestClock.adjust("1 millis");
    expect(yield* Queue.take(f.delivered)).toEqual({ sessionID: sessionB, text: "slow" });
    expect((yield* Queue.take(f.decisions)).questions[0]?.name).toBe("slow");
  }),
);

it.effect("an omitted debounce waits for the default 30-second quiet window", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const decoded = yield* Schema.decodeUnknownEffect(WatchInput)({
      questions: [{ type: "predicate", name: "default", instructions: "Is this relevant?" }],
      transform: 'export default () => "default window"',
    });
    const active = yield* f.relay.watch({ sessionID: sessionA, configuration: decoded });
    yield* Queue.offer(f.source, event());
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("29999 millis");
    expect(yield* Queue.size(f.decisions)).toBe(0);
    expect(yield* Queue.size(f.delivered)).toBe(0);
    yield* TestClock.adjust("1 millis");
    expect((yield* Queue.take(f.delivered)).text).toBe("default window");
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(active);
    expect(decoded.timeoutSeconds).toBe(7200);
  }),
);

it.effect("zero debounce processes messages without advancing the clock", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({
      sessionID: sessionA,
      configuration: { ...configuration(), debounceMilliseconds: 0 },
    });
    const before = yield* Clock.currentTimeMillis;
    for (const incoming of [event("first"), event("second")]) {
      yield* Queue.offer(f.source, incoming);
      yield* TestClock.adjust(0);
      yield* TestClock.adjust(0);
      expect((yield* Queue.take(f.delivered)).text).toBe("literal");
      expect((yield* Queue.take(f.decisions)).input.at(-1)).toEqual({
        type: "json",
        value: {
          root: { chatId: "chat", rootId: "root" },
          events: [incoming],
          history: { items: [{ message_id: "message", content: '{"text":"raw"}' }], hasMore: true },
        },
      });
    }
    expect(yield* Queue.size(f.delivered)).toBe(0);
    expect(yield* Queue.size(f.decisions)).toBe(0);
    expect(yield* Clock.currentTimeMillis).toBe(before);
  }),
);

it.effect(
  "new messages reset conversation quiet time but leave the subscription deadline unchanged",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const active = yield* f.relay.watch({
        sessionID: sessionA,
        configuration: { ...configuration("literal", 1), debounceMilliseconds: 400 },
      });
      const first = event("first");
      const second = event("second");
      yield* Queue.offer(f.source, first);
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("250 millis");
      yield* Queue.offer(f.source, second);
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("150 millis");
      expect(yield* Queue.size(f.decisions)).toBe(0);
      yield* TestClock.adjust("249 millis");
      expect(yield* Queue.size(f.decisions)).toBe(0);
      yield* TestClock.adjust("1 millis");
      expect((yield* Queue.take(f.decisions)).input.at(-1)).toMatchObject({
        type: "json",
        value: { events: [first, second] },
      });
      expect((yield* Queue.take(f.delivered)).text).toBe("literal");
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(active);
      yield* TestClock.adjust("250 millis");
      yield* Queue.offer(f.source, event("third"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("100 millis");
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
      yield* TestClock.adjust("1 second");
      expect(yield* Queue.size(f.decisions)).toBe(0);
      expect(yield* Queue.size(f.delivered)).toBe(0);
    }),
);

it.effect(
  "semantically invalid TypeScript activates and invalid output discards only its batch",
  () =>
    Effect.gen(function* () {
      const f = yield* fixtureWithWorkers(1);
      const active = yield* f.relay.watch({
        sessionID: sessionA,
        configuration: {
          ...configuration(),
          transform: `import type { Transform } from "oc-lark-decisions/transform";
const good: number = "good";
export default ((input) => input.events.some(event => event.content === "rendered:bad-output")
  ? 42
  : good) satisfies Transform;`,
        },
      });
      expect(active.state).toBe("active");
      yield* Queue.offer(f.source, event("bad-output"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      const feedback = yield* Queue.take(f.delivered);
      expect(feedback.sessionID).toBe(sessionA);
      expect(feedback.text).toContain("Transform output error");
      expect(feedback.text).toContain("output:");
      expect(feedback.text).toContain("batch was discarded");
      expect(feedback.text).toContain("watch remains active");
      expect(feedback.text).toContain("lark_watch");
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(active);
      yield* Queue.offer(f.source, event("good-output"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      expect((yield* Queue.take(f.delivered)).text).toBe("good");
    }),
);

it.effect("source termination closes affected subscriptions without restarting", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    yield* f.relay.watch({ sessionID: sessionB, configuration: configuration() });
    yield* Queue.fail(f.source, new LarkError({ operation: "test", cause: "terminated" }));
    yield* TestClock.adjust(0);
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
    expect(yield* f.relay.status({ sessionID: sessionB })).toEqual({ state: "stopped" });
    expect(f.opened()).toBe(1);
    expect(f.closed()).toBe(1);
  }),
);

it.effect("a released source cannot dispatch messages into a later watch using a new source", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("old") });
    yield* f.relay.stop({ sessionID: sessionA });
    const nextSource = yield* Queue.unbounded<LarkEvent, LarkError>();
    f.setSource(nextSource);
    const active = yield* f.relay.watch({
      sessionID: sessionB,
      configuration: configuration("new"),
    });
    const fresh = event("fresh", "fresh-root");
    yield* Queue.offer(f.source, event("stale", "stale-root"));
    yield* Queue.offer(nextSource, fresh);
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    expect(yield* Queue.size(f.decisions)).toBe(1);
    expect((yield* Queue.take(f.decisions)).input.at(-1)).toMatchObject({
      type: "json",
      value: { events: [fresh] },
    });
    expect(yield* Queue.take(f.delivered)).toEqual({ sessionID: sessionB, text: "new" });
    expect(yield* Queue.size(f.delivered)).toBe(0);
    expect(yield* Queue.size(f.source)).toBe(1);
    expect(yield* f.relay.status({ sessionID: sessionB })).toEqual(active);
    expect(f.opened()).toBe(2);
    expect(f.closed()).toBe(1);
  }),
);

it.effect("final source release awaits cleanup", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const release = yield* Deferred.make<void>();
    const stopped = yield* Deferred.make<void>();
    f.setCleanup(Deferred.await(release));
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    const stop = yield* f.relay.stop({ sessionID: sessionA }).pipe(
      Effect.tap(() => Deferred.succeed(stopped, undefined)),
      Effect.forkScoped,
    );
    yield* Queue.take(f.cleanupStarted);
    expect(yield* Deferred.isDone(stopped)).toBe(false);
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(stop);
    expect(f.closed()).toBe(1);
  }),
);

it.effect("delivers an event emitted immediately after readiness", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    f.setInitial(event("immediate"));
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    yield* TestClock.adjust("500 millis");
    expect(yield* Queue.size(f.delivered)).toBe(1);
  }),
);

for (const failure of [
  { source: 'throw new Error("synchronous failure")', message: "synchronous failure" },
  { source: 'return Promise.reject(new Error("rejected promise"))', message: "rejected promise" },
]) {
  it.effect(
    `reports ${failure.message} to the owning session and keeps its worker processing`,
    () =>
      Effect.gen(function* () {
        const f = yield* fixtureWithWorkers(1);
        const active = yield* f.relay.watch({
          sessionID: sessionB,
          configuration: {
            ...configuration(),
            transform: `export default (input) => {
            if (input.events.some(event => event.content === "rendered:bad")) { ${failure.source}; }
            return "later success";
          }`,
          },
        });
        yield* Queue.offer(f.source, event("bad"));
        yield* TestClock.adjust(0);
        yield* TestClock.adjust("500 millis");
        const feedback = yield* Queue.take(f.delivered);
        expect(feedback.sessionID).toBe(sessionB);
        expect(feedback.text).toContain("Transform run error");
        expect(feedback.text).toContain(`run: Error: ${failure.message}`);
        expect(feedback.text).toContain("batch was discarded");
        expect(feedback.text).toContain("watch remains active");
        expect(feedback.text).toContain("Fix the transform and call lark_watch again");
        expect(yield* f.relay.status({ sessionID: sessionB })).toEqual(active);
        expect(yield* Queue.size(f.decisions)).toBe(1);
        yield* Queue.offer(f.source, event("good"));
        yield* TestClock.adjust(0);
        yield* TestClock.adjust("500 millis");
        expect(yield* Queue.take(f.delivered)).toEqual({
          sessionID: sessionB,
          text: "later success",
        });
        expect(yield* Queue.size(f.delivered)).toBe(0);
        expect(yield* Queue.size(f.decisions)).toBe(2);
        expect(yield* f.relay.status({ sessionID: sessionB })).toEqual(active);
      }),
  );
}

it.effect("null skips delivery and whole-call failures discard only their batch", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({
      sessionID: sessionA,
      configuration: { ...configuration(), transform: "export default () => null" },
    });
    yield* Queue.offer(f.source, event("null"));
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    expect(yield* Queue.size(f.delivered)).toBe(0);
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("good") });
    const goodHistory: History = { items: [], hasMore: false };
    f.setHistory(Effect.fail(new LarkError({ operation: "history", cause: "one failed page" })));
    yield* Queue.offer(f.source, event("bad-history"));
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    f.setHistory(Effect.succeed(goodHistory));
    f.setDecision(
      Effect.fail(new OpenCodeError({ operation: "decisions", cause: "one failed call" })),
    );
    yield* Queue.offer(f.source, event("bad-decisions"));
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    expect(yield* Queue.size(f.delivered)).toBe(0);
    f.setDecision(Effect.succeed({ answers: [] }));
    yield* Queue.offer(f.source, event("good"));
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    expect((yield* Queue.take(f.delivered)).text).toBe("good");
    expect((yield* f.relay.status({ sessionID: sessionA })).state).toBe("active");
  }),
);

it.effect("batches by root, bounds workers at four, and replacement interrupts accepted work", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const started = yield* Queue.unbounded<void>();
    const interrupted = yield* Queue.unbounded<void>();
    const release = yield* Deferred.make<History>();
    f.setHistory(
      Queue.offer(started, undefined).pipe(
        Effect.andThen(Deferred.await(release)),
        Effect.onInterrupt(() => Queue.offer(interrupted, undefined)),
      ),
    );
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
    for (let root = 0; root < 6; root++) {
      yield* Queue.offer(f.source, event(`first-${root}`, `root-${root}`));
      yield* Queue.offer(f.source, event(`second-${root}`, `root-${root}`));
    }
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    expect(yield* Queue.size(started)).toBe(4);
    expect(yield* Queue.size(f.roots)).toBe(4);
    expect(yield* Queue.size(f.decisions)).toBe(0);
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("replacement") });
    expect(yield* Queue.size(interrupted)).toBe(4);
    yield* Deferred.succeed(release, { items: [], hasMore: false });
    yield* TestClock.adjust(0);
    expect(yield* Queue.size(f.delivered)).toBe(0);
    expect(f.opened()).toBe(1);
  }),
);

it.effect("replacement expiry is not shortened by the previous deadline", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("old", 1) });
    yield* TestClock.adjust("750 millis");
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("renewed", 10) });
    yield* TestClock.adjust("250 millis");
    expect((yield* f.relay.status({ sessionID: sessionA })).state).toBe("active");
    yield* TestClock.adjust("9750 millis");
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
  }),
);

for (const workers of [1, 2]) {
  it.effect(`${workers} configured workers bounds each watch independently`, () =>
    Effect.gen(function* () {
      const f = yield* fixtureWithWorkers(workers);
      const started = yield* Queue.unbounded<void>();
      const release = yield* Deferred.make<History>();
      f.setHistory(Queue.offer(started, undefined).pipe(Effect.andThen(Deferred.await(release))));
      yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("a") });
      yield* f.relay.watch({ sessionID: sessionB, configuration: configuration("b") });
      for (let root = 0; root < 3; root++) {
        yield* Queue.offer(f.source, event(`message-${root}`, `root-${root}`));
      }
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      expect(yield* Queue.size(started)).toBe(workers * 2);
      expect(yield* Queue.size(f.decisions)).toBe(0);
      yield* Deferred.succeed(release, { items: [], hasMore: false });
      const deliveries = [];
      for (let batch = 0; batch < 6; batch++) deliveries.push(yield* Queue.take(f.delivered));
      expect(deliveries.filter(({ sessionID }) => sessionID === sessionA)).toHaveLength(3);
      expect(deliveries.filter(({ sessionID }) => sessionID === sessionB)).toHaveLength(3);
    }),
  );
}

it.effect(
  "one worker logs failed batches, continues with valid batches, and retains no error state",
  () =>
    Effect.gen(function* () {
      const warnings = yield* Queue.unbounded<unknown>();
      yield* Effect.gen(function* () {
        const f = yield* fixtureWithWorkers(1);
        const before = yield* f.relay.watch({
          sessionID: sessionA,
          configuration: configuration("good"),
        });
        for (const failure of ["history", "decisions", "queue"]) {
          if (failure === "history") {
            f.setHistory(
              Effect.fail(new LarkError({ operation: "history", cause: "bad history" })),
            );
          } else if (failure === "decisions") {
            f.setDecision(
              Effect.fail(new OpenCodeError({ operation: "decisions", cause: "bad decisions" })),
            );
          } else {
            f.setQueue(Effect.fail(new OpenCodeError({ operation: "queue", cause: "bad prompt" })));
          }
          yield* Queue.offer(f.source, event(`bad-${failure}`));
          yield* TestClock.adjust(0);
          yield* TestClock.adjust("500 millis");
          expect(yield* Queue.take(warnings)).toEqual(["Lark batch discarded", expect.anything()]);
          expect(yield* Queue.size(f.delivered)).toBe(0);
          const status = yield* f.relay.status({ sessionID: sessionA });
          expect(status).toEqual(before);
          expect(status).not.toHaveProperty("diagnostic");
          expect(status).not.toHaveProperty("error");
          f.setHistory(Effect.succeed({ items: [], hasMore: false }));
          f.setDecision(Effect.succeed({ answers: [] }));
          f.setQueue(Effect.void);
          yield* Queue.offer(f.source, event(`good-${failure}`));
          yield* TestClock.adjust(0);
          yield* TestClock.adjust("500 millis");
          expect((yield* Queue.take(f.delivered)).text).toBe("good");
          expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(before);
        }
      }).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make((options) => {
              if (options.logLevel === "Warn") Queue.offerUnsafe(warnings, options.message);
            }),
          ]),
        ),
      );
    }),
);

it.effect("failed transform feedback is logged once and does not stop the worker", () =>
  Effect.gen(function* () {
    const warnings = yield* Queue.unbounded<unknown>();
    yield* Effect.gen(function* () {
      const f = yield* fixtureWithWorkers(1);
      const active = yield* f.relay.watch({
        sessionID: sessionA,
        configuration: {
          ...configuration(),
          transform: `export default (input) => {
            if (input.events.some(event => event.content === "rendered:bad")) throw new Error("bad transform");
            return "good";
          }`,
        },
      });
      let attempts = 0;
      f.setQueue(
        Effect.sync(() => {
          attempts++;
        }).pipe(
          Effect.andThen(
            Effect.fail(new OpenCodeError({ operation: "queue", cause: "unavailable" })),
          ),
        ),
      );
      yield* Queue.offer(f.source, event("bad"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      expect(yield* Queue.take(warnings)).toEqual(["Lark batch discarded", expect.anything()]);
      expect(attempts).toBe(1);
      expect(yield* Queue.size(warnings)).toBe(0);
      expect(yield* Queue.size(f.delivered)).toBe(0);
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(active);
      f.setQueue(Effect.void);
      yield* Queue.offer(f.source, event("good"));
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      expect(yield* Queue.take(f.delivered)).toEqual({ sessionID: sessionA, text: "good" });
    }).pipe(
      Effect.provide(
        Logger.layer([
          Logger.make((options) => {
            if (options.logLevel === "Warn") Queue.offerUnsafe(warnings, options.message);
          }),
        ]),
      ),
    );
  }),
);

it.effect("an old expiry already queued behind replacement cannot stop the replacement", () =>
  Effect.gen(function* () {
    const f = yield* fixture;
    const working = yield* Deferred.make<void>();
    const closing = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const candidateLoaded = yield* Deferred.make<void>();
    f.setHistory(
      Deferred.succeed(working, undefined).pipe(
        Effect.andThen(Effect.never),
        Effect.onInterrupt(() =>
          Deferred.succeed(closing, undefined).pipe(Effect.andThen(Deferred.await(release))),
        ),
      ),
    );
    yield* f.relay.watch({ sessionID: sessionB, configuration: configuration("blocking", 100) });
    yield* Queue.offer(f.source, event());
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("500 millis");
    yield* Deferred.await(working);
    yield* f.relay.watch({ sessionID: sessionA, configuration: configuration("old", 1) });
    const stop = yield* f.relay.stop({ sessionID: sessionB }).pipe(Effect.forkScoped);
    yield* Deferred.await(closing);
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        Object.defineProperty(globalThis, "__relayReplacementLoaded", {
          configurable: true,
          value: () => Effect.runSync(Deferred.succeed(candidateLoaded, undefined)),
        });
      }),
      () => Effect.sync(() => Reflect.deleteProperty(globalThis, "__relayReplacementLoaded")),
    );
    const replacement = yield* f.relay
      .watch({
        sessionID: sessionA,
        configuration: {
          ...configuration("renewed", 10),
          transform:
            'Reflect.get(globalThis, "__relayReplacementLoaded")(); export default () => "renewed";',
        },
      })
      .pipe(Effect.forkScoped);
    yield* Deferred.await(candidateLoaded);
    yield* TestClock.adjust(0);
    yield* TestClock.adjust("1 second");
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(stop);
    const renewed = yield* Fiber.join(replacement);
    expect(yield* f.relay.status({ sessionID: sessionA })).toEqual(renewed);
    expect(renewed.state).toBe("active");
    expect(f.opened()).toBe(1);
    expect(f.closed()).toBe(0);
  }),
);

it.effect(
  "rejects a prepared candidate when the shared source terminates while activation waits for old work",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture;
      const working = yield* Deferred.make<void>();
      const closing = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const candidateLoaded = yield* Deferred.make<void>();
      f.setHistory(
        Deferred.succeed(working, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() =>
            Deferred.succeed(closing, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        ),
      );
      yield* f.relay.watch({ sessionID: sessionA, configuration: configuration() });
      yield* Queue.offer(f.source, event());
      yield* TestClock.adjust(0);
      yield* TestClock.adjust("500 millis");
      yield* Deferred.await(working);
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          Object.defineProperty(globalThis, "__relayCandidateLoaded", {
            configurable: true,
            value: () => Effect.runSync(Deferred.succeed(candidateLoaded, undefined)),
          });
        }),
        () => Effect.sync(() => Reflect.deleteProperty(globalThis, "__relayCandidateLoaded")),
      );
      const watch = yield* f.relay
        .watch({
          sessionID: sessionA,
          configuration: {
            ...configuration("candidate"),
            transform:
              'Reflect.get(globalThis, "__relayCandidateLoaded")(); export default () => "candidate";',
          },
        })
        .pipe(Effect.result, Effect.forkScoped);
      yield* Deferred.await(candidateLoaded);
      yield* Deferred.await(closing);
      yield* Queue.fail(
        f.source,
        new LarkError({ operation: "test", cause: "source died during replacement" }),
      );
      yield* TestClock.adjust(0);
      yield* Deferred.succeed(release, undefined);
      expect((yield* Fiber.join(watch))._tag).toBe("Failure");
      expect(yield* f.relay.status({ sessionID: sessionA })).toEqual({ state: "stopped" });
    }),
);
