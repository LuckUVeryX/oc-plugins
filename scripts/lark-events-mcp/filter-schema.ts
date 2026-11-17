import { isObject } from "effect/Predicate";

export const inputSchemaDocument = (schema: unknown): Record<string, unknown> => {
  const properties: Record<string, unknown> = {};
  const walk = (node: unknown, path: string): void => {
    if (!isObject(node)) return;
    const type = node["type"];
    if (type === "string" || type === "number" || type === "integer" || type === "boolean") {
      if (!path) return;
      const scalar = (value: unknown) =>
        typeof value === "string" || typeof value === "number" || typeof value === "boolean";
      const constant = node["const"];
      const values = scalar(constant)
        ? [constant]
        : Array.isArray(node["enum"])
          ? node["enum"].filter(scalar)
          : undefined;
      const leaf = { type, ...(values === undefined ? {} : { enum: values }) };
      properties[path] = { anyOf: [leaf, { type: "array", items: leaf, minItems: 1 }] };
      return;
    }
    if (type === "array") return;
    const children = node["properties"];
    if (isObject(children))
      for (const [name, child] of Object.entries(children))
        walk(child, path ? `${path}.${name}` : name);
  };
  walk(schema, "");
  return {
    type: "object",
    properties: {
      filter: {
        type: "object",
        description: "AND across paths, OR within a nonempty array, using strict equality.",
        properties,
        additionalProperties: false,
      },
    },
    additionalProperties: false,
  };
};
