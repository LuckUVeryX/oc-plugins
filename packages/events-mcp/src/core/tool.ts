import { Effect, type JsonSchema, Schema } from "effect";
import { ToolError, type Tool } from "./source.ts";

const QueryValue = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]);

export const GetInput = Schema.Struct({
  path: Schema.String,
  query: Schema.optionalKey(Schema.Record(Schema.String, QueryValue)),
});
export type GetInput = typeof GetInput.Type;

export const makeTool = <Input>(options: {
  readonly description: string;
  readonly input: Schema.Decoder<Input>;
  readonly run: (input: Input) => Effect.Effect<Schema.Json, ToolError>;
}): Tool => ({
  description: options.description,
  input: options.input,
  run: (input) =>
    Schema.decodeUnknownEffect(options.input)(input).pipe(
      Effect.mapError((issue) => new ToolError({ message: issue.message })),
      Effect.flatMap(options.run),
    ),
});

export const inputJsonSchema = (tool: Tool): JsonSchema.JsonSchema => {
  const { schema, definitions } = Schema.toJsonSchemaDocument(tool.input);
  return Object.keys(definitions).length === 0 ? schema : { ...schema, $defs: definitions };
};
