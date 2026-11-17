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
    readonly list: () => Effect.Effect<ReadonlyArray<EventDefinition>>;
    readonly schema: (name: string) => Effect.Effect<EventDefinition, Error>;
  }
>()("lark-events-mcp/LarkCatalog") {}

export const catalogLayer = Layer.effect(
  LarkCatalog,
  Effect.gen(function* () {
    const catalog = yield* Schema.decodeUnknownEffect(Catalog)(generated);
    const byName = new Map(catalog.events.map((event) => [event.name, event]));
    return LarkCatalog.of({
      list: () => Effect.succeed(catalog.events),
      schema: (name) => {
        const event = byName.get(name);
        return event ? Effect.succeed(event) : Effect.fail(new Error(`unknown event: ${name}`));
      },
    });
  }),
);
