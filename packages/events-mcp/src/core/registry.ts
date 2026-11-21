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
import type { EventKind, Integration } from "./integration.ts";

export interface RegisteredEventKind extends EventKind {
  readonly name: string;
  readonly paramsSchema: ReturnType<typeof compileParams>;
  readonly filter: CompiledFilter;
}

export interface Interface {
  readonly integrations: ReadonlyArray<Integration>;
  readonly events: ReadonlyArray<RegisteredEventKind>;
  readonly get: (name: string) => RegisteredEventKind | undefined;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Registry") {}

const compileParams = (event: EventKind, name: string) => {
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
  integrations: Effect.Effect<ReadonlyArray<Integration>, never, R>,
): Layer.Layer<Service, never, Exclude<R, Scope.Scope>> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const built = yield* integrations;
      const byName = new Map<string, RegisteredEventKind>();
      for (const integration of built)
        for (const event of integration.events) {
          const name = `${integration.name}.${event.name}`;
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
        integrations: built,
        events: [...byName.values()],
        get: (name) => byName.get(name),
      });
    }),
  );

export * as Registry from "./registry.ts";
