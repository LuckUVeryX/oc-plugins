import { Effect, Schema } from "effect";
import { isObject } from "effect/Predicate";
import { Decisions } from "./decisions.ts";

const Scalar = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);
type Scalar = typeof Scalar.Type;
const isScalar = Schema.is(Scalar);
const WatchArguments = Schema.Struct({
  watchId: Schema.NonEmptyString,
  filter: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
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
        description: "Choose an ID unique among active watches.",
      },
      filter: {
        type: "object",
        description:
          "Match raw payload dot paths. All paths must match; an array matches any listed value.",
        properties,
        additionalProperties: false,
      },
      transform: {
        type: "string",
        description:
          "Run a Code Mode program per matching event. Read the raw payload at input.event.data; input.event also has eventId, timestamp, and timestampOrigin. Return a string to deliver it; return null, undefined, or nothing to suppress it. Other results, errors, and exceeded limits also suppress it. Limits: 65536-byte source, 1048576-byte input, 1000 ms, 16384 bytes for the returned string as JSON, quotes included. Omit for raw delivery.",
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
    const decoded = yield* Schema.decodeUnknownEffect(WatchArguments)(input, {
      onExcessProperty: "error",
    }).pipe(Effect.mapError((issue) => issue.message));
    const { watchId, filter: raw = {}, transform, decisions } = decoded;
    if (decisions !== undefined && transform === undefined)
      return yield* Effect.fail("Decisions requires transform; add transform or omit decisions");
    const leaves = new Map(filterLeaves(schema).map((leaf) => [leaf.path, leaf]));
    const filter: Array<readonly [string, ReadonlyArray<Scalar>]> = [];
    for (const [path, value] of Object.entries(raw)) {
      const leaf = leaves.get(path);
      if (!leaf)
        return yield* Effect.fail(`Unknown filter path: ${path}; choose a path from inputSchema`);
      const values = isScalar(value) ? [value] : Array.isArray(value) ? value : undefined;
      if (!values || values.length === 0 || !values.every(isScalar))
        return yield* Effect.fail(`Filter ${path} must be a scalar or non-empty array of scalars`);
      if (!values.every((scalar) => matchesLeaf(scalar, leaf)))
        return yield* Effect.fail(
          `Invalid value for filter ${path}; use a value allowed by inputSchema`,
        );
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
