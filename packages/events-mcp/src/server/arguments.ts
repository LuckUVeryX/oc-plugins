import { Effect, Schema } from "effect";
import { MAX_EVENT_BYTES } from "../limits.ts";

export class Invalid extends Schema.TaggedError<Invalid>()("Arguments.Invalid", {
  message: Schema.String,
}) {}

const WatchId = Schema.NonEmptyString.pipe(Schema.brand("WatchId"));

const Target = Schema.Struct({ target: Schema.optionalKey(Schema.NonEmptyString) });

const WatchArguments = Schema.Struct({
  ...Target.fields,
  code: Schema.String,
  id: WatchId,
  expiresInSeconds: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  ),
});

const idProperty = {
  type: "string",
  minLength: 1,
  description:
    "Your name for this watch, unique among open watches. The `watches` tool lists it, and `unwatch` takes it.",
};

const codeProperty = (toolPaths: ReadonlyArray<string>) => {
  const tools = [
    ...toolPaths,
    "tools.watch.state.get",
    "tools.watch.state.set",
    "tools.events.decide",
  ].join(", ");
  return {
    type: "string",
    description: `JavaScript that runs for each event and decides whether to wake you. Read the payload at input.event.data and its metadata at input.event.eventId and input.event.timestamp. It can await any of ${tools}. Return null to drop the event, or a string or JSON value to wake you with it. The delivered event is capped at ${MAX_EVENT_BYTES} bytes, so return only what the session needs. One event's code has a time limit, and a slow event delays the ones behind it. Wake on every event with \`return input.event.data\`.`,
  };
};

const expiresInSecondsProperty = (defaultExpiresInSeconds: number) => ({
  type: "integer",
  minimum: 1,
  description: `How long the watch stays open, in seconds. When it elapses the watch closes and sends nothing more. Defaults to ${defaultExpiresInSeconds} seconds.`,
});

export const targetSchemaDocument = (
  target: { readonly description: string; readonly required: boolean } | undefined,
) => ({
  type: "object",
  properties:
    target === undefined
      ? {}
      : { target: { type: "string", minLength: 1, description: target.description } },
  required: target?.required === true ? ["target"] : [],
  additionalProperties: false,
});

/** Adds what a client without automations needs. */
export const watchSchemaDocument = (
  target: ReturnType<typeof targetSchemaDocument>,
  options: {
    readonly toolPaths: ReadonlyArray<string>;
    readonly defaultExpiresInSeconds: number;
  },
) => ({
  ...target,
  properties: {
    id: idProperty,
    ...target.properties,
    code: codeProperty(options.toolPaths),
    expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
  },
  required: ["id", ...target.required, "code"],
});

const decode =
  <A>(schema: Schema.Codec<A, unknown>) =>
  (value: unknown) =>
    Schema.decodeUnknownEffect(schema)(value, { onExcessProperty: "error" }).pipe(
      Effect.mapError((issue) => new Invalid({ message: issue.message })),
    );

export const decodeTarget = decode(Target);
export const decodeWatch = decode(WatchArguments);
