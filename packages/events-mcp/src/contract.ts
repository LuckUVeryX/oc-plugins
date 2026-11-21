import { Effect, JsonSchema, Schema } from "effect";
import { isObject } from "effect/Predicate";

export const WatchId = Schema.NonEmptyString.pipe(Schema.brand("WatchId"));
export type WatchId = typeof WatchId.Type;

const Scalar = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
type Scalar = typeof Scalar.Type;
const isScalar = Schema.is(Scalar);
const WatchArguments = Schema.Struct({
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

const leafDocument = (leaf: FilterPath): Record<string, unknown> => ({
  type: leaf.type,
  ...(leaf.values === undefined ? {} : { enum: leaf.values }),
});

export const inputSchemaDocument = (options: {
  readonly filter: CompiledFilter;
  readonly params: JsonSchema.JsonSchema;
  readonly toolPaths: ReadonlyArray<string>;
  readonly decide: boolean;
  readonly defaultExpiresInSeconds: number;
}): Record<string, unknown> => {
  const properties: Record<string, unknown> = {};
  for (const [path, leaf] of options.filter)
    properties[path] = {
      anyOf: [leafDocument(leaf), { type: "array", items: leafDocument(leaf), minItems: 1 }],
    };
  const tools = [
    ...options.toolPaths,
    "tools.watch.state",
    ...(options.decide ? ["tools.events.decide"] : []),
  ].join(", ");
  return {
    type: "object",
    properties: {
      watchId: {
        type: "string",
        minLength: 1,
        description: "Unique among active watches.",
      },
      params: {
        ...options.params,
        description: options.params["description"] ?? "Subscription params.",
      },
      filter: {
        type: "object",
        description:
          "Match payload fields by dot path before prepare runs. Each path maps to a value or a list of accepted values; all paths must match.",
        properties,
        additionalProperties: false,
      },
      prepare: {
        type: "string",
        description: `Code Mode gate. Reads input.event.data; returns JSON for transform, or null to drop. Tools: ${tools}. Pass-through: return input.event.data`,
      },
      transform: {
        type: "string",
        description:
          "Code Mode without tools. Reads input.event and input.prepared; returns the wake text, or null to drop. Pass-through: return JSON.stringify(input.prepared)",
      },
      expiresInSeconds: {
        type: "integer",
        minimum: 1,
        description: `How long the watch stays open, in seconds. Defaults to ${options.defaultExpiresInSeconds} seconds.`,
      },
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
    }).pipe(Effect.mapError((issue) => issue.message));
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
        return yield* Effect.fail(`Unknown filter path: ${path}; choose a path from inputSchema`);
      const values = isScalar(value) ? [value] : Array.isArray(value) ? value : undefined;
      if (!values || values.length === 0 || !values.every(isScalar))
        return yield* Effect.fail(`Filter ${path} must be a scalar or non-empty array of scalars`);
      if (!values.every((scalar) => matchesLeaf(scalar, leaf)))
        return yield* Effect.fail(
          `Invalid value for filter ${path}; use a value allowed by inputSchema`,
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
