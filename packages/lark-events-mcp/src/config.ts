import { Config, Schema } from "effect";

export const watchMaxSeconds = Config.schema(
  Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  "LARK_WATCH_MAX_SECONDS",
).pipe(Config.withDefault(3600));
