import { Effect, Schema } from "effect";
import { isObject } from "effect/Predicate";
import { Decisions } from "./decisions.ts";

const Scalar = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
type Scalar = typeof Scalar.Type;
const isScalar = Schema.is(Scalar);
const WatchArguments = Schema.Struct({
  watchId: Schema.NonEmptyString,
  filter: Schema.optionalKey(Schema.Unknown),
  transform: Schema.optionalKey(Schema.String),
  decisions: Schema.optionalKey(Decisions),
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

const strictDocument = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(strictDocument);
  if (!isObject(value)) return value;
  return Object.fromEntries(
    Object.entries(value).map(([key, entry]) => [
      key,
      key === "additionalProperties" && entry === true && "properties" in value
        ? false
        : strictDocument(entry),
    ]),
  );
};

export const inputSchemaDocument = (schema: unknown): Record<string, unknown> => {
  const decisions = Schema.toJsonSchemaDocument(Decisions);
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
        description:
          "Choose an ID unique among active Lark event watches. Reuse it with stop_watch.",
      },
      filter: {
        type: "object",
        description:
          "Match raw payload fields by dot path. All paths must match exactly. An array allows any listed value for that path.",
        properties,
        additionalProperties: false,
      },
      transform: {
        type: "string",
        description:
          "Format or suppress each matching event with a Code Mode program. Read input.event.data for the payload. Return a string to deliver that string, or null to suppress the event. Omit for raw delivery.",
      },
      decisions: strictDocument(decisions.schema),
    },
    $defs: strictDocument(decisions.definitions),
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
    const error = (message: string) => Effect.fail(message);
    if (!isObject(input) || Array.isArray(input))
      return yield* error("stream arguments must be an object");
    if (Object.keys(input).some((key) => !Object.hasOwn(WatchArguments.fields, key)))
      return yield* error("unknown stream argument");
    const decoded = yield* Schema.decodeUnknownEffect(WatchArguments)(input, {
      onExcessProperty: "error",
    }).pipe(Effect.option);
    if (decoded._tag === "None") return yield* error("invalid watch arguments");
    const { watchId, filter: raw = {}, transform, decisions } = decoded.value;
    if (decisions !== undefined && transform === undefined)
      return yield* error("decisions requires transform");
    if (!isObject(raw) || Array.isArray(raw)) return yield* error("filter must be an object");
    const leaves = new Map(filterLeaves(schema).map((leaf) => [leaf.path, leaf]));
    const filter: Array<readonly [string, ReadonlyArray<Scalar>]> = [];
    for (const [path, value] of Object.entries(raw)) {
      const leaf = leaves.get(path);
      if (!leaf) return yield* error(`unknown filter path: ${path}`);
      const values = isScalar(value) ? [value] : Array.isArray(value) ? value : undefined;
      if (!values || values.length === 0 || !values.every(isScalar))
        return yield* error(`filter ${path} must be a scalar or non-empty array of scalars`);
      if (!values.every((scalar) => matchesLeaf(scalar, leaf)))
        return yield* error(`invalid value for filter ${path}`);
      filter.push([path, values]);
    }
    return { filter, watchId, transform, decisions };
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
