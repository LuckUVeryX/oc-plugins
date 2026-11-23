import { Context, Effect, Layer, type Scope } from "effect";
import type { EventType, Integration } from "./contract.ts";

export interface Interface {
  readonly integrations: ReadonlyArray<Integration>;
  /** Each event is named `<integration>.<event>`. */
  readonly events: ReadonlyArray<EventType>;
  readonly get: (name: string) => EventType | undefined;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Registry") {}

export const layer = <R>(
  integrations: Effect.Effect<ReadonlyArray<Integration>, never, R>,
): Layer.Layer<Service, never, Exclude<R, Scope.Scope>> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const built = yield* integrations;
      const byName = new Map<string, EventType>();
      for (const integration of built)
        for (const event of integration.events) {
          const name = `${integration.name}.${event.name}`;
          if (byName.has(name))
            return yield* Effect.die(new Error(`Duplicate event names: ${name}`));
          byName.set(name, { ...event, name });
        }
      return Service.of({
        integrations: built,
        events: [...byName.values()],
        get: (name) => byName.get(name),
      });
    }),
  );

export * as Registry from "./registry.ts";
