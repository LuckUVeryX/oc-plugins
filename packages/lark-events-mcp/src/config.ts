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

export const eventTimeoutSeconds = Config.schema(
  Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  "LARK_EVENT_TIMEOUT_SECONDS",
).pipe(Config.withDefault(120));
