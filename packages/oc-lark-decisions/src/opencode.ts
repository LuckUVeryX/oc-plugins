import type { Plugin } from "@opencode/plugin/effect";
import { Generate } from "@opencode/schema/generate";
import type { Session } from "@opencode/schema/session";
import { Context, Effect, Layer, Schema, Stream } from "effect";

export const Options = Schema.Struct({
  model: Generate.DecisionRequest.fields.model,
  workers: Schema.Int.check(Schema.isGreaterThan(0)).pipe(
    Schema.withDecodingDefault(Effect.succeed(4)),
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
  }
>()("oc-lark-decisions/OpenCode") {}

export const openCodeLayer = (ctx: Plugin.Context, options: Options) =>
  Layer.effect(
    OpenCode,
    Effect.sync(() =>
      OpenCode.of({
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
      }),
    ),
  );
