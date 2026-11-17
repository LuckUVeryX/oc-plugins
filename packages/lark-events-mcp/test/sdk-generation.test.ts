import { describe, expect, it } from "vitest";
import { JsonSchema, Predicate, Schema, SchemaRepresentation } from "effect";
import catalog from "../src/generated/events.json";
import { inputSchemaDocument } from "../../../scripts/lark-events-mcp/filter-schema.ts";

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

function property(schema: JsonSchema.JsonSchema, ...path: string[]): JsonSchema.JsonSchema {
  let current = schema;
  for (const key of path) {
    if (!Predicate.isObject(current.properties)) {
      throw new Error(`No properties while looking up ${path.join(".")}`);
    }
    const child = current.properties[key];
    if (!Predicate.isObject(child)) throw new Error(`Missing schema for ${path.join(".")}`);
    current = child;
  }
  return current;
}

describe("generated SDK event catalog", () => {
  it("generates strict subscription inputs for every event", () => {
    for (const event of catalog.events) {
      expect(event.inputSchema).toEqual(inputSchemaDocument(event.payloadSchema));
      const schema = Schema.make<Schema.Codec<unknown, unknown>>(
        SchemaRepresentation.fromJsonSchemaDocument(
          JsonSchema.fromSchemaDraft2020_12(event.inputSchema),
        ).ast,
      );
      expect(Schema.decodeUnknownResult(schema, { onExcessProperty: "error" })({})._tag).toBe(
        "Success",
      );
    }
  });

  it("keeps scalar paths and constraints without traversing arrays", () => {
    const schema = inputSchemaDocument({
      type: "object",
      properties: {
        kind: { type: "string", const: "one" },
        state: { type: "string", enum: ["a", "b"] },
        count: { type: "integer" },
        items: { type: "array", items: { type: "object", properties: { id: { type: "string" } } } },
      },
    });
    const decode = Schema.decodeUnknownResult(
      Schema.make<Schema.Codec<unknown, unknown>>(
        SchemaRepresentation.fromJsonSchemaDocument(JsonSchema.fromSchemaDraft2020_12(schema)).ast,
      ),
      { onExcessProperty: "error" },
    );
    for (const value of [
      {},
      { filter: {} },
      { filter: { kind: "one", state: ["a", "b"], count: 1 } },
    ])
      expect(decode(value)._tag).toBe("Success");
    for (const value of [
      { filter: { kind: "two" } },
      { filter: { state: "c" } },
      { filter: { "items.id": "x" } },
      { filter: { count: 1.5 } },
      { filter: { state: [] } },
      { extra: true },
    ])
      expect(decode(value)._tag).toBe("Failure");
  });

  it("contains all SDK handlers, including inherited callbacks", () => {
    expect(catalog.sdkVersion).toBe("1.76.0");
    expect(catalog.events).toHaveLength(227);
    expect(new Set(catalog.events.map((event) => event.name)).size).toBe(227);
    expect(catalog.events.some((event) => event.name === "leave_approvalV2")).toBe(true);
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

  it("converts tuple lengths and item types", () => {
    const schema = property(payload("leave_approvalV2"), "leave_detail");
    expect(accepts(schema, [[1, 2]])).toBe(true);
    expect(accepts(schema, [[1]])).toBe(false);
    expect(accepts(schema, [[1, 2, 3]])).toBe(false);
    expect(accepts(schema, [["1", 2]])).toBe(false);
    const rooms = property(payload("third_party_meeting_room_event_created"), "meeting_rooms");
    expect(accepts(rooms, [{ open_id: "a" }, { open_id: "b" }])).toBe(true);
    expect(accepts(rooms, [{ open_id: "a" }])).toBe(false);
  });

  it("preserves unknown JSON and typed maps", () => {
    const unknown = property(payload("group_setting_update"), "before_change");
    for (const value of [null, 3, "x", [], { arbitrary: true }]) {
      expect(accepts(unknown, value)).toBe(true);
    }
    const resources = property(payload("leave_approvalV2"), "i18n_resources");
    if (!Predicate.isObject(resources.items)) {
      throw new Error("Missing i18n resource item schema");
    }
    const texts = property(resources.items, "texts");
    expect(accepts(texts, { label: "Leave" })).toBe(true);
    expect(accepts(texts, { label: 3 })).toBe(false);
  });
});
