import { Context, Effect, Layer, type Scope } from "effect";
import type { EventType, Source } from "./contract.ts";

export interface Interface {
  readonly sources: ReadonlyArray<Source>;
  /** Each event is named `<source>.<event>`. */
  readonly events: ReadonlyArray<EventType>;
  readonly get: (name: string) => EventType | undefined;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Registry") {}

export const layer = <R>(
  sources: Effect.Effect<ReadonlyArray<Source>, never, R>,
): Layer.Layer<Service, never, Exclude<R, Scope.Scope>> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const built = yield* sources;
      const byName = new Map<string, EventType>();
      for (const source of built)
        for (const event of source.events) {
          const name = `${source.name}.${event.name}`;
          if (byName.has(name))
            return yield* Effect.die(new Error(`Duplicate event names: ${name}`));
          byName.set(name, { ...event, name });
        }
      return Service.of({
        sources: built,
        events: [...byName.values()],
        get: (name) => byName.get(name),
      });
    }),
  );

export * as Registry from "./registry.ts";
