import { Effect, JsonSchema, Schema } from "effect";
import { isObject } from "effect/Predicate";
import { MAX_EVENT_BYTES } from "./limits.ts";

export class Invalid extends Schema.TaggedError<Invalid>()("Arguments.Invalid", {
  message: Schema.String,
}) {}

export const WatchId = Schema.NonEmptyString.pipe(Schema.brand("WatchId"));
export type WatchId = typeof WatchId.Type;

const Scalar = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
type Scalar = typeof Scalar.Type;
const isScalar = Schema.is(Scalar);
export const WatchArguments = Schema.Struct({
  watchId: WatchId,
  params: Schema.optionalKey(Schema.JsonObject),
  filter: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  prepare: Schema.String,
  transform: Schema.String,
  expiresInSeconds: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  ),
});

interface FilterPath {
  readonly segments: ReadonlyArray<string>;
  readonly type: "string" | "number" | "integer" | "boolean";
  readonly values: ReadonlyArray<Scalar> | undefined;
}

export type CompiledFilter = ReadonlyMap<string, FilterPath>;

export type FilterEntry = readonly [ReadonlyArray<string>, ReadonlyArray<Scalar>];
export type Filter = ReadonlyArray<FilterEntry>;

export const compileFilter = (payload: unknown): CompiledFilter => {
  const paths = new Map<string, FilterPath>();
  const walk = (node: unknown, segments: ReadonlyArray<string>): void => {
    if (!isObject(node)) return;
    const type = node["type"];
    if (type === "string" || type === "number" || type === "integer" || type === "boolean") {
      const constant = node["const"];
      const values = isScalar(constant)
        ? [constant]
        : Array.isArray(node["enum"])
          ? node["enum"].filter(isScalar)
          : undefined;
      if (segments.length > 0) paths.set(segments.join("."), { segments, type, values });
      return;
    }
    if (type === "array") return;
    const properties = node["properties"];
    if (isObject(properties))
      for (const [name, child] of Object.entries(properties)) walk(child, [...segments, name]);
  };
  walk(payload, []);
  return paths;
};

const leafDocument = (leaf: FilterPath): JsonSchema.JsonSchema => ({
  type: leaf.type,
  ...(leaf.values === undefined ? {} : { enum: leaf.values }),
});

export const watchIdProperty = {
  type: "string",
  minLength: 1,
  description:
    "Your name for this watch, unique among open watches. It appears in list_watches and stop_watch.",
};

export const prepareProperty = (toolPaths: ReadonlyArray<string>) => {
  const tools = [
    ...toolPaths,
    "tools.watch.state.get",
    "tools.watch.state.set",
    "tools.events.decide",
  ].join(", ");
  return {
    type: "string",
    description: `JavaScript that runs for each payload passing filter and decides whether to wake you. Read the payload at input.event.data and its metadata at input.event.eventId and input.event.timestamp. It can await any of ${tools}. Return JSON for transform, or null to drop this event. Prepare and transform share the step time limit, and a slow event delays the ones behind it. Pass the payload through with \`return input.event.data\`.`,
  };
};

export const transformProperty = {
  type: "string",
  description: `JavaScript that turns prepare's result into the text that wakes you. Read input.prepared and input.event, then return a string, or null to drop this event. It runs without tools. The delivered event is capped at ${MAX_EVENT_BYTES} bytes, so return only what the session needs. Pass everything through with \`return JSON.stringify(input.prepared)\`.`,
};

export const expiresInSecondsProperty = (defaultExpiresInSeconds: number) => ({
  type: "integer",
  minimum: 1,
  description: `How long the watch stays open, in seconds. When it elapses the watch closes and sends nothing more. Defaults to ${defaultExpiresInSeconds} seconds.`,
});

export const filterProperty = (options: {
  readonly properties?: Record<string, JsonSchema.JsonSchema>;
  readonly hint?: string;
}) => ({
  type: "object",
  description: [
    "Continue only with payloads that match. Each key is a dot path into this event's payload with one value or a list of accepted values, and every key you set must match. A payload that fails is dropped before prepare runs, and paths inside arrays cannot be filtered.",
    ...(options.hint === undefined ? [] : [options.hint]),
    "Omit it to accept every payload.",
  ].join(" "),
  ...(options.properties === undefined
    ? {}
    : { properties: options.properties, additionalProperties: false }),
});

export const inputSchemaDocument = (options: {
  readonly filter: CompiledFilter;
  readonly params: JsonSchema.JsonSchema;
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}): Record<string, unknown> => {
  const properties: Record<string, JsonSchema.JsonSchema> = {};
  for (const [path, leaf] of options.filter)
    properties[path] = {
      anyOf: [leafDocument(leaf), { type: "array", items: leafDocument(leaf), minItems: 1 }],
    };
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
      filter: filterProperty({ properties }),
      prepare: prepareProperty(options.toolPaths),
      transform: transformProperty,
      expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
    },
    required: ["watchId", "prepare", "transform"],
    additionalProperties: false,
  };
};

const leafTypes: Record<FilterPath["type"], (value: Scalar) => boolean> = {
  string: (value) => typeof value === "string",
  number: (value) => typeof value === "number" && Number.isFinite(value),
  integer: (value) => Number.isInteger(value),
  boolean: (value) => typeof value === "boolean",
};

const matchesLeaf = (value: Scalar, leaf: FilterPath): boolean =>
  (leaf.values === undefined || leaf.values.includes(value)) && leafTypes[leaf.type](value);

export const decodeArguments = (options: {
  readonly filter: CompiledFilter;
  readonly value: unknown;
}) =>
  Effect.gen(function* () {
    const decoded = yield* Schema.decodeUnknownEffect(WatchArguments)(options.value, {
      onExcessProperty: "error",
    }).pipe(Effect.mapError((issue) => new Invalid({ message: issue.message })));
    const {
      watchId,
      params = {},
      filter: raw = {},
      prepare,
      transform,
      expiresInSeconds,
    } = decoded;
    const filter: Array<FilterEntry> = [];
    for (const [path, value] of Object.entries(raw)) {
      const leaf = options.filter.get(path);
      if (!leaf)
        return yield* Effect.fail(
          new Invalid({
            message: `Unknown filter path: ${path}; choose a path from inputSchema`,
          }),
        );
      const values = isScalar(value) ? [value] : Array.isArray(value) ? value : undefined;
      if (!values || values.length === 0 || !values.every(isScalar))
        return yield* Effect.fail(
          new Invalid({
            message: `Filter ${path} must be a scalar or non-empty array of scalars`,
          }),
        );
      if (!values.every((scalar) => matchesLeaf(scalar, leaf)))
        return yield* Effect.fail(
          new Invalid({
            message: `Invalid value for filter ${path}; use a value allowed by inputSchema`,
          }),
        );
      filter.push([leaf.segments, values]);
    }
    return { filter, params, prepare, transform, watchId, expiresInSeconds };
  });

export const matchesFilter = (filter: Filter, payload: unknown): boolean =>
  filter.every(([segments, values]) => {
    let node = payload;
    for (const key of segments) {
      if (!isObject(node) || Array.isArray(node)) return false;
      node = node[key];
    }
    return values.some((value) => node === value);
  });
