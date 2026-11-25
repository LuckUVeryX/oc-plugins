import { Schema } from "effect";

export const Version = Schema.String.check(
  Schema.isPattern(/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/),
);

export const Hello = Schema.Struct({
  harness: Schema.String,
  version: Version,
});
export type Hello = typeof Hello.Type;

export const Reply = Schema.Union([
  Schema.Struct({ status: Schema.Literal("accepted"), version: Version }),
  Schema.Struct({ status: Schema.Literal("replaced") }),
]);
export type Reply = typeof Reply.Type;

export const encodeReply = (reply: Reply) => `${JSON.stringify(reply)}\n`;
