import { Effect, JsonSchema, Schema } from "effect";
import { isObject } from "effect/Predicate";
import { MAX_EVENT_BYTES } from "./limits.ts";

export class Invalid extends Schema.TaggedError<Invalid>()("Arguments.Invalid", {
  message: Schema.String,
}) {}

export const WatchId = Schema.NonEmptyString.pipe(Schema.brand("WatchId"));
export type WatchId = typeof WatchId.Type;

export const WatchArguments = Schema.Struct({
  watchId: WatchId,
  params: Schema.optionalKey(Schema.JsonObject),
  target: Schema.optionalKey(Schema.NonEmptyString),
  script: Schema.String,
  expiresInSeconds: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  ),
});

export const watchIdProperty = {
  type: "string",
  minLength: 1,
  description:
    "Your name for this watch, unique among open watches. The `watches` tool lists it, and `unwatch` takes it.",
};

export const scriptProperty = (toolPaths: ReadonlyArray<string>) => {
  const tools = [
    ...toolPaths,
    "tools.watch.state.get",
    "tools.watch.state.set",
    "tools.events.decide",
  ].join(", ");
  return {
    type: "string",
    description: `JavaScript that runs for each event and decides whether to wake you. Read the payload at input.event.data and its metadata at input.event.eventId and input.event.timestamp. It can await any of ${tools}. Return null to drop the event, or a string or JSON value to wake you with it. JSON is sent as JSON.stringify of the value. The delivered event is capped at ${MAX_EVENT_BYTES} bytes, so return only what the session needs. One event's script has a time limit, and a slow event delays the ones behind it. Wake on every event with \`return input.event.data\`.`,
  };
};

export const expiresInSecondsProperty = (defaultExpiresInSeconds: number) => ({
  type: "integer",
  minimum: 1,
  description: `How long the watch stays open, in seconds. When it elapses the watch closes and sends nothing more. Defaults to ${defaultExpiresInSeconds} seconds.`,
});

export const inputSchemaDocument = (options: {
  readonly params: JsonSchema.JsonSchema;
  readonly target?: string | undefined;
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}): Record<string, unknown> => {
  const paramsProperties = options.params["properties"];
  const paramsCount = isObject(paramsProperties) ? Object.keys(paramsProperties).length : 0;
  return {
    type: "object",
    properties: {
      watchId: watchIdProperty,
      params: {
        ...options.params,
        description:
          options.params["description"] ??
          (paramsCount === 0
            ? "This event takes no params."
            : "Selects the resource to watch. The schema lists the fields."),
      },
      ...(options.target === undefined
        ? {}
        : {
            target: {
              type: "string",
              minLength: 1,
              description: options.target,
            },
          }),
      script: scriptProperty(options.toolPaths),
      expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
    },
    required: ["watchId", "script"],
    additionalProperties: false,
  };
};

export const decodeArguments = (value: unknown) =>
  Schema.decodeUnknownEffect(WatchArguments)(value, { onExcessProperty: "error" }).pipe(
    Effect.mapError((issue) => new Invalid({ message: issue.message })),
  );
