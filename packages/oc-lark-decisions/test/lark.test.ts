import { BunServices } from "@effect/platform-bun";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Logger, Queue, Scope, Stream } from "effect";
import { LarkError } from "../src/lark.ts";
import { controlledCLI } from "./fixtures.ts";

it.live(
  "owns the real CLI process, recognizes readiness, decodes NDJSON, and awaits process cleanup",
  () =>
    Effect.gen(function* () {
      const scope = yield* Scope.fork(yield* Scope.Scope);
      const { lark, directory, fs } = yield* controlledCLI(`
if [ "$1" = event ]; then
  printf '%s\\n' "$@" > "$(dirname "$0")/event-args"
  trap 'printf cleaned > "$(dirname "$0")/cleaned"; exit 0' TERM
  printf '[event] ready event_key=im.message.receive_v1\\n' >&2
  printf '%s\\n' '{"type":"im.message.receive_v1","chat_id":"chat","message_id":"message","root_id":"root","thread_id":"thread","content":"rendered"}'
  read waiting
else
  printf '%s\\n' '{"ok":true,"data":{"items":[{"message_id":"message","body":{"content":"{\\"text\\":\\"raw\\"}"}}],"has_more":true}}'
fi`);
      const stream = yield* lark.open().pipe(Scope.provide(scope));
      expect(yield* fs.readFileString(`${directory}/event-args`)).toBe(
        "event\nconsume\nim.message.receive_v1\n--as\nbot\n",
      );
      const events = yield* stream.pipe(Stream.take(1), Stream.runCollect);
      expect(events).toEqual([
        {
          type: "im.message.receive_v1",
          chat_id: "chat",
          message_id: "message",
          root_id: "root",
          thread_id: "thread",
          content: "rendered",
        },
      ]);
      expect(yield* lark.history({ chatId: "chat", rootId: "root" })).toEqual({
        items: [{ message_id: "message", content: '{"text":"raw"}' }],
        hasMore: true,
      });
      yield* Scope.close(scope, Exit.void);
      expect(yield* fs.readFileString(`${directory}/cleaned`)).toBe("cleaned");
    }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live(
  "skips raw interactive cards with a warning and keeps delivering flat messages on the active source",
  () =>
    Effect.gen(function* () {
      const messages = yield* Queue.unbounded<unknown>();
      const warnings = yield* Queue.unbounded<{
        readonly level: string;
        readonly message: unknown;
      }>();
      const ended = yield* Deferred.make<void>();
      const first = {
        type: "im.message.receive_v1",
        chat_id: "chat",
        message_id: "first",
        content: "before card",
      };
      const card = {
        schema: "2.0",
        header: { event_type: "im.message.receive_v1", event_id: "card" },
        event: {
          message: {
            message_id: "card",
            chat_id: "chat",
            message_type: "interactive",
            content: '{"type":"template"}',
          },
        },
      };
      const second = { ...first, message_id: "second", content: "after card" };
      yield* Effect.gen(function* () {
        const { lark } = yield* controlledCLI(`
trap 'exit 0' TERM
printf '[event] ready event_key=im.message.receive_v1\\n' >&2
printf '%s\\n' '${JSON.stringify(first)}' '${JSON.stringify(card)}' '${JSON.stringify(second)}'
read waiting`);
        const stream = yield* lark.open();
        yield* stream.pipe(
          Stream.runForEach((message) => Queue.offer(messages, message)),
          Effect.onExit(() => Deferred.succeed(ended, undefined)),
          Effect.forkScoped,
        );
        expect([yield* Queue.take(messages), yield* Queue.take(messages)]).toEqual([first, second]);
        expect(yield* Queue.take(warnings)).toEqual({
          level: "Warn",
          message: ["Skipping unsupported raw interactive-card message envelope"],
        });
        expect(yield* Queue.size(warnings)).toBe(0);
        expect(yield* Deferred.isDone(ended)).toBe(false);
      }).pipe(
        Effect.provide(
          Logger.layer([
            Logger.make((options) => {
              Queue.offerUnsafe(warnings, { level: options.logLevel, message: options.message });
            }),
          ]),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("does not skip unrelated raw envelopes or malformed JSON", () =>
  Effect.gen(function* () {
    for (const payload of [
      "invalid JSON",
      JSON.stringify({
        header: { event_type: "im.message.receive_v1" },
        event: { message: { message_type: "text" } },
      }),
      JSON.stringify({
        header: { event_type: "different.event" },
        event: { message: { message_type: "interactive" } },
      }),
    ]) {
      yield* Effect.gen(function* () {
        const { lark } = yield* controlledCLI(`
trap 'exit 0' TERM
printf '[event] ready event_key=im.message.receive_v1\\n' >&2
printf '%s\\n' '${payload}'
read waiting`);
        const stream = yield* lark.open();
        const result = yield* Stream.runDrain(stream).pipe(Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(String(result.failure)).toContain(
            payload === "invalid JSON" ? "event JSON" : "event:",
          );
          expect(result.failure.operation).toBe("consume");
          expect(result.failure.cause).toBeInstanceOf(LarkError);
          if (result.failure.cause instanceof LarkError) {
            expect(result.failure.cause.operation).toBe(
              payload === "invalid JSON" ? "event JSON" : "event",
            );
          }
        }
      }).pipe(Effect.scoped);
    }
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("preserves history JSON error operation wrapping for malformed CLI responses", () =>
  Effect.gen(function* () {
    const { lark } = yield* controlledCLI("printf 'invalid JSON'");
    const result = yield* lark.history({ chatId: "chat", rootId: "root" }).pipe(Effect.result);
    expect(result._tag).toBe("Failure");
    if (result._tag === "Failure") {
      expect(result.failure.operation).toBe("history");
      expect(result.failure.cause).toBeInstanceOf(LarkError);
      if (result.failure.cause instanceof LarkError) {
        expect(result.failure.cause.operation).toBe("history JSON");
      }
    }
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("reports process exit before readiness and malformed event data", () =>
  Effect.gen(function* () {
    const early = yield* controlledCLI("printf 'not ready\\n' >&2\nexit 7");
    expect((yield* early.lark.open().pipe(Effect.scoped, Effect.exit))._tag).toBe("Failure");
    const malformed = yield* controlledCLI(`
printf '[event] ready event_key=im.message.receive_v1\\n' >&2
printf 'invalid JSON\\n'
read waiting`);
    const stream = yield* malformed.lark.open();
    expect((yield* Stream.runDrain(stream).pipe(Effect.exit))._tag).toBe("Failure");
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);
