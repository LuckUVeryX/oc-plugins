import { Generate } from "@opencode/schema/generate";
import { Schema } from "effect";
import { Root } from "./transform.ts";

export const Outcome = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("skipped") }),
  Schema.Struct({ kind: Schema.Literal("queued") }),
  Schema.Struct({
    kind: Schema.Literal("failed"),
    stage: Schema.Literals(["history", "decision", "transform", "queue"]),
    message: Schema.String,
  }),
]);
export type Outcome = Schema.Schema.Type<typeof Outcome>;

export const BatchRecord = Schema.Struct({
  completedAt: Schema.Finite,
  root: Root,
  eventCount: Schema.Int.check(Schema.isGreaterThan(0)),
  firstMessageId: Schema.String,
  lastMessageId: Schema.String,
  answers: Schema.optionalKey(Generate.DecisionResult.fields.answers),
  outcome: Outcome,
});
export interface BatchRecord extends Schema.Schema.Type<typeof BatchRecord> {}

export const BatchHistory = Schema.Array(BatchRecord);

export const retain = (previous: readonly BatchRecord[], record: BatchRecord) => {
  const records = [record, ...previous].slice(0, 100);
  while (new TextEncoder().encode(JSON.stringify(records)).byteLength > 64 * 1024) {
    records.pop();
  }
  return records;
};
