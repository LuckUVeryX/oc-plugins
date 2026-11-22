import { Config, Schema } from "effect";
import { Generate } from "@opencode/schema/generate";

export const decisionModel = Config.schema(
  Schema.fromJsonString(Generate.DecisionRequest.fields.model),
  "EVENTS_DECISIONS_MODEL",
).pipe(Config.option);
