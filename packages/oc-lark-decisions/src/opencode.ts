import type { Plugin } from "@opencode/plugin/effect";
import { Generate } from "@opencode/schema/generate";
import type { Session } from "@opencode/schema/session";
import { Cause, Context, Effect, Layer, Schema, Stream } from "effect";
import { BatchHistory, type BatchRecord, retain } from "./history.ts";

export const Options = Schema.Struct({
  model: Generate.DecisionRequest.fields.model,
  maxEventAgeSeconds: Schema.Finite.check(Schema.isGreaterThan(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(300)),
  ),
});
export interface Options extends Schema.Schema.Type<typeof Options> {}

export class OpenCodeError extends Schema.TaggedError<OpenCodeError>()("OpenCodeError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {
  override get message() {
    return `${this.operation}: ${String(this.cause)}`;
  }
}

export type DecisionInput = Pick<Generate.DecisionRequest, "questions" | "input">;

export class OpenCode extends Context.Service<
  OpenCode,
  {
    readonly decide: (
      input: DecisionInput,
    ) => Effect.Effect<Generate.DecisionResult, OpenCodeError>;
    readonly queue: (input: {
      readonly sessionID: Session.ID;
      readonly text: string;
    }) => Effect.Effect<void, OpenCodeError>;
    readonly deletedSessions: Stream.Stream<Session.ID, OpenCodeError>;
    readonly history: (
      sessionID: Session.ID,
    ) => Effect.Effect<readonly BatchRecord[], OpenCodeError>;
    readonly recordBatch: (sessionID: Session.ID, record: BatchRecord) => Effect.Effect<void>;
    readonly removeHistory: (sessionID: Session.ID) => Effect.Effect<void>;
  }
>()("oc-lark-decisions/OpenCode") {}

export const openCodeLayer = (ctx: Plugin.Context, options: Options) =>
  Layer.effect(
    OpenCode,
    Effect.sync(() => {
      const key = (sessionID: Session.ID) => `history/${sessionID}`;
      const storageError = <A>(effect: Effect.Effect<A>) =>
        effect.pipe(
          Effect.catchCause((cause) =>
            Cause.hasInterrupts(cause)
              ? Effect.failCause(cause)
              : Effect.fail(
                  new OpenCodeError({ operation: "history", cause: Cause.pretty(cause) }),
                ),
          ),
        );
      const history = Effect.fn("OpenCode.history")(function* (sessionID: Session.ID) {
        const stored = yield* storageError(Effect.suspend(() => ctx.storage.get(key(sessionID))));
        if (stored === undefined) return [];
        return yield* Schema.decodeUnknownEffect(BatchHistory)(stored).pipe(
          Effect.mapError((cause) => new OpenCodeError({ operation: "history", cause })),
        );
      });
      return OpenCode.of({
        history,
        recordBatch: Effect.fn("OpenCode.recordBatch")(
          function* (sessionID, record) {
            const previous = yield* history(sessionID);
            const records = retain(previous, record);
            if (records.length === 0) {
              return yield* Effect.logWarning("Lark batch history record exceeds 64 KiB");
            }
            const json = yield* Schema.encodeEffect(Schema.toCodecJson(BatchHistory))(records);
            yield* storageError(Effect.suspend(() => ctx.storage.set(key(sessionID), json)));
          },
          Effect.catch((error) => Effect.logWarning("Lark batch history write failed", error)),
        ),
        removeHistory: Effect.fn("OpenCode.removeHistory")(
          (sessionID) => storageError(Effect.suspend(() => ctx.storage.remove(key(sessionID)))),
          Effect.catch((error) => Effect.logWarning("Lark batch history deletion failed", error)),
        ),
        decide: Effect.fn("OpenCode.decide")((input: DecisionInput) =>
          ctx.experimental.generate
            .decisions({ ...input, model: options.model })
            .pipe(Effect.mapError((cause) => new OpenCodeError({ operation: "decisions", cause }))),
        ),
        queue: Effect.fn("OpenCode.queue")(
          (input: { readonly sessionID: Session.ID; readonly text: string }) =>
            ctx.session
              .prompt({
                sessionID: input.sessionID,
                text: input.text,
                delivery: "queue",
              })
              .pipe(
                Effect.asVoid,
                Effect.mapError((cause) => new OpenCodeError({ operation: "queue", cause })),
              ),
        ),
        deletedSessions: ctx.event.subscribe({ types: ["session.deleted"] }).pipe(
          Stream.map((event) => event.data.sessionID),
          Stream.mapError((cause) => new OpenCodeError({ operation: "deletedSessions", cause })),
        ),
      });
    }),
  );
