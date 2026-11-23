import { Effect, Schema } from "effect";
import { MAX_EVENT_BYTES } from "../limits.ts";

export class Invalid extends Schema.TaggedError<Invalid>()("Arguments.Invalid", {
  message: Schema.String,
}) {}

const WatchId = Schema.NonEmptyString.pipe(Schema.brand("WatchId"));

const WatchArguments = Schema.Struct({
  watchId: WatchId,
  target: Schema.optionalKey(Schema.NonEmptyString),
  script: Schema.String,
  expiresInSeconds: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  ),
});

const watchIdProperty = {
  type: "string",
  minLength: 1,
  description:
    "Your name for this watch, unique among open watches. The `watches` tool lists it, and `unwatch` takes it.",
};

const scriptProperty = (toolPaths: ReadonlyArray<string>) => {
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

const expiresInSecondsProperty = (defaultExpiresInSeconds: number) => ({
  type: "integer",
  minimum: 1,
  description: `How long the watch stays open, in seconds. When it elapses the watch closes and sends nothing more. Defaults to ${defaultExpiresInSeconds} seconds.`,
});

/** The JSON Schema for one watch's arguments, without the event name. */
export const inputSchemaDocument = (options: {
  readonly target: { readonly description: string; readonly required: boolean } | undefined;
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}) => ({
  type: "object",
  properties: {
    watchId: watchIdProperty,
    ...(options.target === undefined
      ? {}
      : { target: { type: "string", minLength: 1, description: options.target.description } }),
    script: scriptProperty(options.toolPaths),
    expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
  },
  required:
    options.target?.required === true ? ["watchId", "target", "script"] : ["watchId", "script"],
  additionalProperties: false,
});

export const decodeArguments = (value: unknown) =>
  Schema.decodeUnknownEffect(WatchArguments)(value, { onExcessProperty: "error" }).pipe(
    Effect.mapError((issue) => new Invalid({ message: issue.message })),
  );
