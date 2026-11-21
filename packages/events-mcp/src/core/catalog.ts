import {
  Context,
  Effect,
  JsonSchema,
  Layer,
  Schema,
  SchemaRepresentation,
  type Scope,
} from "effect";
import { compileFilter, type CompiledFilter } from "./arguments.ts";
import type { EventType, Source } from "./source.ts";

export interface RegisteredEvent extends EventType {
  readonly name: string;
  readonly paramsSchema: ReturnType<typeof compileParams>;
  readonly filter: CompiledFilter;
}

export interface Interface {
  readonly sources: ReadonlyArray<Source>;
  readonly events: ReadonlyArray<RegisteredEvent>;
  readonly get: (name: string) => RegisteredEvent | undefined;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Catalog") {}

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

export const layer = <R>(
  sources: Effect.Effect<ReadonlyArray<Source>, never, R>,
): Layer.Layer<Service, never, Exclude<R, Scope.Scope>> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const built = yield* sources;
      const byName = new Map<string, RegisteredEvent>();
      for (const source of built)
        for (const event of source.events) {
          const name = `${source.name}.${event.name}`;
          if (byName.has(name))
            return yield* Effect.die(new Error(`Duplicate event names: ${name}`));
          byName.set(name, {
            ...event,
            name,
            paramsSchema: compileParams(event, name),
            filter: compileFilter(event.payload),
          });
        }
      return Service.of({
        sources: built,
        events: [...byName.values()],
        get: (name) => byName.get(name),
      });
    }),
  );

export * as Catalog from "./catalog.ts";
