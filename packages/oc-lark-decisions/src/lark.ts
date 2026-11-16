import { Context, Deferred, Effect, Layer, Queue, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { type History, HistoryMessage, LarkEvent, type Root } from "./transform.ts";

export class LarkError extends Schema.TaggedError<LarkError>()("LarkError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `${this.operation}: ${String(this.cause)}`;
  }
}

export class Lark extends Context.Service<
  Lark,
  {
    readonly open: () => Effect.Effect<
      Stream.Stream<LarkEvent, LarkError>,
      LarkError,
      import("effect").Scope.Scope
    >;
    readonly history: (root: Root) => Effect.Effect<History, LarkError>;
  }
>()("oc-lark-relay/Lark") {}

const HistoryResponse = Schema.Struct({
  ok: Schema.Literal(true),
  data: Schema.Struct({
    items: Schema.Array(
      Schema.Struct({
        ...HistoryMessage.fields,
        content: Schema.optionalKey(Schema.String),
        body: Schema.Struct({ content: Schema.String }),
      }),
    ),
    has_more: Schema.Boolean,
  }),
});

const RawInteractiveCard = Schema.Struct({
  header: Schema.Struct({ event_type: Schema.Literal("im.message.receive_v1") }),
  event: Schema.Struct({
    message: Schema.Struct({ message_type: Schema.Literal("interactive") }),
  }),
});

const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

export const larkLayer = Layer.effect(
  Lark,
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const error = (operation: string) => (cause: unknown) => new LarkError({ operation, cause });
    return Lark.of({
      open: Effect.fn("Lark.open")(function* () {
        const process = yield* spawner
          .spawn(
            ChildProcess.make(
              "lark-cli",
              ["event", "consume", "im.message.receive_v1", "--as", "bot"],
              {
                stdin: "pipe",
              },
            ),
          )
          .pipe(Effect.mapError(error("open")));
        const ready = yield* Deferred.make<void, LarkError>();
        const events = yield* Queue.unbounded<LarkEvent, LarkError>();
        yield* Effect.addFinalizer(() => Queue.shutdown(events));
        yield* process.stderr.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.runForEach((line) =>
            line === "[event] ready event_key=im.message.receive_v1"
              ? Deferred.succeed(ready, undefined)
              : Effect.logDebug(line),
          ),
          Effect.mapError(error("stderr")),
          Effect.catch((cause) =>
            Effect.all([Deferred.fail(ready, cause), Queue.fail(events, cause)], { discard: true }),
          ),
          Effect.forkScoped,
        );
        yield* process.stdout.pipe(
          Stream.decodeText(),
          Stream.splitLines,
          Stream.filter((line) => line.trim().length > 0),
          Stream.mapEffect((line) => decodeJson(line).pipe(Effect.mapError(error("event JSON")))),
          Stream.filterEffect((value) =>
            Schema.is(RawInteractiveCard)(value)
              ? Effect.logWarning(
                  "Skipping unsupported raw interactive-card message envelope",
                ).pipe(Effect.as(false))
              : Effect.succeed(true),
          ),
          Stream.mapEffect((value) =>
            Schema.decodeUnknownEffect(LarkEvent)(value).pipe(Effect.mapError(error("event"))),
          ),
          Stream.runForEach((event) => Queue.offer(events, event)),
          Effect.flatMap(() => process.exitCode),
          Effect.mapError(error("consume")),
          Effect.flatMap((code) => Effect.fail(error("consume")(`CLI exited with code ${code}`))),
          Effect.catch((cause) =>
            Effect.all([Deferred.fail(ready, cause), Queue.fail(events, cause)], { discard: true }),
          ),
          Effect.forkScoped,
        );
        yield* Deferred.await(ready);
        return Stream.fromQueue(events);
      }),
      history: Effect.fn("Lark.history")(function* (root: Root) {
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const process = yield* spawner.spawn(
              ChildProcess.make("lark-cli", [
                "api",
                "GET",
                "/open-apis/im/v1/messages",
                "--params",
                JSON.stringify({
                  container_id_type: "chat",
                  container_id: root.chatId,
                  sort_type: "ByCreateTimeDesc",
                  page_size: 5,
                }),
                "--json",
              ]),
            );
            const [stdout, stderr, code] = yield* Effect.all(
              [
                process.stdout.pipe(Stream.decodeText(), Stream.mkString),
                process.stderr.pipe(Stream.decodeText(), Stream.mkString),
                process.exitCode,
              ],
              { concurrency: "unbounded" },
            );
            if (code !== 0)
              return yield* Effect.fail(new LarkError({ operation: "history", cause: stderr }));
            const json = yield* decodeJson(stdout).pipe(Effect.mapError(error("history JSON")));
            const response = yield* Schema.decodeUnknownEffect(HistoryResponse)(json);
            return {
              items: response.data.items.map(({ body, ...message }) => ({
                ...message,
                content: body.content,
              })),
              hasMore: response.data.has_more,
            };
          }),
        ).pipe(Effect.mapError(error("history")));
      }),
    });
  }),
);
