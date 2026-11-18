import { Config, Schema } from "effect";
import { Generate } from "@opencode/schema/generate";

export const decisionModel = Config.schema(
  Schema.fromJsonString(Generate.DecisionRequest.fields.model),
  "LARK_DECISIONS_MODEL",
).pipe(Config.option);

export const watchMaxSeconds = Config.schema(
  Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  "LARK_WATCH_MAX_SECONDS",
).pipe(Config.withDefault(3600));

const positiveInteger = Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0));

export const transformConfig = Config.all({
  timeoutMs: Config.schema(positiveInteger, "LARK_TRANSFORM_TIMEOUT_MS").pipe(
    Config.withDefault(1000),
  ),
  sourceBytes: Config.schema(positiveInteger, "LARK_TRANSFORM_SOURCE_BYTES").pipe(
    Config.withDefault(65536),
  ),
  inputBytes: Config.schema(positiveInteger, "LARK_TRANSFORM_INPUT_BYTES").pipe(
    Config.withDefault(1048576),
  ),
  outputBytes: Config.schema(positiveInteger, "LARK_TRANSFORM_OUTPUT_BYTES").pipe(
    Config.withDefault(16384),
  ),
});
