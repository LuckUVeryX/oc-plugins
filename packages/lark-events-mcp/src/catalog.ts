import { Context, Effect, Layer, Schema } from "effect";
import generated from "./generated/events.json";

const EventDefinition = Schema.Struct({
  name: Schema.String,
  description: Schema.optionalKey(Schema.String),
  payloadSchema: Schema.Record(Schema.String, Schema.Unknown),
});
export interface EventDefinition extends Schema.Schema.Type<typeof EventDefinition> {}

const Catalog = Schema.Struct({ events: Schema.Array(EventDefinition) });

export class LarkCatalog extends Context.Service<
  LarkCatalog,
  {
    readonly events: ReadonlyArray<EventDefinition>;
    readonly get: (name: string) => EventDefinition | undefined;
  }
>()("lark-events-mcp/LarkCatalog") {}

export const catalogLayer = Layer.effect(
  LarkCatalog,
  Effect.gen(function* () {
    const catalog = yield* Schema.decodeUnknownEffect(Catalog)(generated);
    const byName = new Map(catalog.events.map((event) => [event.name, event]));
    return LarkCatalog.of({
      events: catalog.events,
      get: (name) => byName.get(name),
    });
  }),
);
