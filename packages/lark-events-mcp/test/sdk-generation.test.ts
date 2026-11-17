import { describe, expect, it } from "vitest";
import { JsonSchema, Schema, SchemaRepresentation } from "effect";
import catalog from "../src/generated/events.json";

function payload(name: string): JsonSchema.JsonSchema {
  const event = catalog.events.find((candidate) => candidate.name === name);
  if (!event) throw new Error(`Missing SDK event ${name}`);
  return event.payloadSchema as JsonSchema.JsonSchema;
}

function accepts(schema: JsonSchema.JsonSchema, value: unknown): boolean {
  return Schema.is(
    Schema.make(
      SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12(schema)).ast,
    ),
  )(value);
}

describe("generated SDK event catalog", () => {
  it("contains direct SDK callbacks without inherited legacy handlers", () => {
    expect(catalog.sdkVersion).toBe("1.76.0");
    expect(catalog.events).toHaveLength(203);
    expect(new Set(catalog.events.map((event) => event.name)).size).toBe(203);
    expect(catalog.events.some((event) => event.name === "im.message.receive_v1")).toBe(true);
    expect(catalog.events.some((event) => event.name === "leave_approvalV2")).toBe(false);
  });

  it("retains enums, consts and dispatcher metadata", () => {
    const schema = payload("calendar.calendar.acl.created_v4");
    const value = {
      acl_id: "acl",
      scope: { type: "user" },
      role: "writer",
      fixture_metadata: true,
    };
    expect(accepts(schema, value)).toBe(true);
    expect(accepts(schema, { ...value, role: "invented" })).toBe(false);
    expect(accepts(schema, { ...value, scope: { type: "invented" } })).toBe(false);
  });
});
