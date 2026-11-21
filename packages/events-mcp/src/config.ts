import { Config, Schema } from "effect";
import { Generate } from "@opencode/schema/generate";

const seconds = Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0));

export const decisionModel = Config.schema(
  Schema.fromJsonString(Generate.DecisionRequest.fields.model),
  "EVENTS_DECISIONS_MODEL",
).pipe(Config.option);

export const watchDefaultSeconds = Config.schema(seconds, "EVENTS_WATCH_DEFAULT_SECONDS").pipe(
  Config.withDefault(3600),
);

export const stepTimeoutSeconds = Config.schema(seconds, "EVENTS_STEP_TIMEOUT_SECONDS").pipe(
  Config.withDefault(120),
);
