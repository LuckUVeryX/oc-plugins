import { Effect, JsonSchema, Schema, SchemaRepresentation } from "effect";
import { compileFilter, type CompiledFilter } from "./arguments.ts";
import type { EventType, Source } from "./source.ts";

export interface RegisteredEvent extends EventType {
  readonly name: string;
  readonly paramsSchema: ReturnType<typeof compileParams>;
  readonly filter: CompiledFilter;
}

export interface Catalog {
  readonly sources: ReadonlyArray<Source>;
  readonly events: ReadonlyArray<RegisteredEvent>;
  readonly get: (name: string) => RegisteredEvent | undefined;
}

const compileParams = (event: EventType, name: string) => {
  try {
    const document = JsonSchema.fromSchemaDraft2020_12(event.params);
    const schema: Schema.Codec<unknown> = Schema.make(
      // The catalog is generated and committed here, so its patterns are trusted.
      SchemaRepresentation.fromJsonSchemaDocument(document, { patterns: "apply" }).ast,
    );
    return Schema.decodeTo(Schema.JsonObject)(schema);
  } catch (error) {
    throw new Error(`Invalid params schema for ${name}: ${String(error)}`);
  }
};

export const makeCatalog = (sources: ReadonlyArray<Source>): Effect.Effect<Catalog> =>
  Effect.gen(function* () {
    const byName = new Map<string, RegisteredEvent>();
    for (const source of sources)
      for (const event of source.events) {
        const name = `${source.name}.${event.name}`;
        if (byName.has(name)) return yield* Effect.die(new Error(`Duplicate event names: ${name}`));
        byName.set(name, {
          ...event,
          name,
          paramsSchema: compileParams(event, name),
          filter: compileFilter(event.payload),
        });
      }
    return {
      sources,
      events: [...byName.values()],
      get: (name) => byName.get(name),
    };
  });
