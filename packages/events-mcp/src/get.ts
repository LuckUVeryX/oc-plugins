import { Schema } from "effect";

const QueryValue = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]);

export const GetInput = Schema.Struct({
  path: Schema.String,
  query: Schema.optionalKey(Schema.Record(Schema.String, QueryValue)),
});
export type GetInput = typeof GetInput.Type;
