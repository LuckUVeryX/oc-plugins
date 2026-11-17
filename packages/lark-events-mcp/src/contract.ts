import { Effect, Schema } from "effect";
import { isObject } from "effect/Predicate";

const Scalar = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
type Scalar = typeof Scalar.Type;
const isScalar = Schema.is(Scalar);
const WatchArguments = Schema.Struct({
  watchId: Schema.NonEmptyString,
  durationSeconds: Schema.optionalKey(Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0))),
  debounce: Schema.optionalKey(
    Schema.Struct({
      waitSeconds: Schema.optionalKey(
        Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
      ),
      maxWaitSeconds: Schema.optionalKey(
        Schema.Number.check(Schema.isInt(), Schema.isGreaterThanOrEqualTo(0)),
      ),
    }),
  ),
  filter: Schema.optionalKey(Schema.Unknown),
});

interface FilterLeaf {
  readonly path: string;
  readonly type: "string" | "number" | "integer" | "boolean";
  readonly values: ReadonlyArray<Scalar> | undefined;
}

export type Filter = ReadonlyArray<readonly [string, ReadonlyArray<Scalar>]>;

const filterLeaves = (schema: unknown): ReadonlyArray<FilterLeaf> => {
  const leaves: Array<FilterLeaf> = [];
  const walk = (node: unknown, prefix: string): void => {
    if (!isObject(node)) return;
    const type = node["type"];
    if (type === "string" || type === "number" || type === "integer" || type === "boolean") {
      const constant = node["const"];
      const values = isScalar(constant)
        ? [constant]
        : Array.isArray(node["enum"])
          ? node["enum"].filter(isScalar)
          : undefined;
      if (prefix) leaves.push({ path: prefix, type, values });
      return;
    }
    if (type === "array") return;
    const properties = node["properties"];
    if (isObject(properties))
      for (const [name, child] of Object.entries(properties))
        walk(child, prefix ? `${prefix}.${name}` : name);
  };
  walk(schema, "");
  return leaves;
};

const leafDocument = (leaf: FilterLeaf): Record<string, unknown> => ({
  type: leaf.type,
  ...(leaf.values === undefined ? {} : { enum: leaf.values }),
});

export const inputSchemaDocument = (schema: unknown): Record<string, unknown> => {
  const properties: Record<string, unknown> = {};
  for (const leaf of filterLeaves(schema))
    properties[leaf.path] = {
      anyOf: [leafDocument(leaf), { type: "array", items: leafDocument(leaf), minItems: 1 }],
    };
  return {
    type: "object",
    properties: {
      watchId: {
        type: "string",
        minLength: 1,
        description: "Unique among active watches in this server process.",
      },
      durationSeconds: {
        type: "integer",
        minimum: 1,
        default: 3600,
        description: "Lifetime in seconds from acceptance, including connection readiness.",
      },
      debounce: {
        type: "object",
        description: "Batch timing in seconds. maxWaitSeconds must be at least waitSeconds.",
        properties: {
          waitSeconds: {
            type: "integer",
            minimum: 0,
            default: 5,
            description: "Quiet period before flushing. Zero delivers events immediately.",
          },
          maxWaitSeconds: {
            type: "integer",
            minimum: 0,
            default: 30,
            description: "Maximum batch age measured from its first event.",
          },
        },
        additionalProperties: false,
      },
      filter: {
        type: "object",
        description: "Match all paths with strict equality. An array matches any listed value.",
        properties,
        additionalProperties: false,
      },
    },
    required: ["watchId"],
    additionalProperties: false,
  };
};

const matchesLeaf = (value: Scalar, leaf: FilterLeaf): boolean => {
  if (leaf.values !== undefined && !leaf.values.includes(value)) return false;
  switch (leaf.type) {
    case "string":
      return typeof value === "string";
    case "number":
      return typeof value === "number" && Number.isFinite(value);
    case "integer":
      return typeof value === "number" && Number.isInteger(value);
    case "boolean":
      return typeof value === "boolean";
  }
};

export const decodeArguments = (schema: unknown, input: unknown) =>
  Effect.gen(function* () {
    const error = (message: string) => ({ _tag: "error" as const, message });
    if (!isObject(input) || Array.isArray(input))
      return error("stream arguments must be an object");
    if (
      Object.keys(input).some(
        (key) => !["filter", "watchId", "durationSeconds", "debounce"].includes(key),
      )
    )
      return error("unknown stream argument");
    if (
      isObject(input["debounce"]) &&
      Object.keys(input["debounce"]).some((key) => !["waitSeconds", "maxWaitSeconds"].includes(key))
    )
      return error("unknown debounce argument");
    const decoded = yield* Schema.decodeUnknownEffect(WatchArguments)(input).pipe(Effect.option);
    if (decoded._tag === "None") return error("invalid watch arguments");
    const { watchId, durationSeconds = 3600, filter: raw = {}, debounce } = decoded.value;
    const { waitSeconds = 5, maxWaitSeconds = 30 } = debounce ?? {};
    if (maxWaitSeconds < waitSeconds) return error("maxWaitSeconds must be at least waitSeconds");
    if (!isObject(raw) || Array.isArray(raw)) return error("filter must be an object");
    const leaves = new Map(filterLeaves(schema).map((leaf) => [leaf.path, leaf]));
    const filter: Array<readonly [string, ReadonlyArray<Scalar>]> = [];
    for (const [path, value] of Object.entries(raw)) {
      const leaf = leaves.get(path);
      if (!leaf) return error(`unknown filter path: ${path}`);
      const values = isScalar(value) ? [value] : Array.isArray(value) ? value : undefined;
      if (!values || values.length === 0 || !values.every(isScalar))
        return error(`filter ${path} must be a scalar or non-empty array of scalars`);
      if (!values.every((scalar) => matchesLeaf(scalar, leaf)))
        return error(`invalid value for filter ${path}`);
      filter.push([path, values]);
    }
    return {
      _tag: "ok" as const,
      value: { filter, watchId, durationSeconds, waitSeconds, maxWaitSeconds },
    };
  });

export const matchesFilter = (filter: Filter, payload: unknown): boolean =>
  filter.every(([path, values]) => {
    let node = payload;
    for (const key of path.split(".")) {
      if (!isObject(node) || Array.isArray(node)) return false;
      node = node[key];
    }
    return values.some((value) => node === value);
  });
