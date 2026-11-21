import { Effect, Schema } from "effect";
import { type EventType, type Source, SourceName } from "../core/source.ts";
import { LarkApi } from "./api.ts";
import { LarkConnection } from "./connection.ts";
import generated from "./events.json";
import { makeGetTool } from "./get.ts";

const GeneratedEvents = Schema.Struct({
  events: Schema.Array(
    Schema.Struct({
      name: Schema.NonEmptyString,
      description: Schema.optionalKey(Schema.String),
      payloadSchema: Schema.JsonObject,
    }),
  ),
});

const catalog = Schema.decodeUnknownSync(GeneratedEvents)(generated);

export const source: Effect.Effect<Source, never, LarkApi.Service | LarkConnection.Service> =
  Effect.gen(function* () {
    const api = yield* LarkApi.Service;
    const connection = yield* LarkConnection.Service;
    const events: ReadonlyArray<EventType> = catalog.events.map((event) => ({
      name: event.name,
      description: event.description ?? event.name,
      params: { type: "object", properties: {}, additionalProperties: false },
      payload: event.payloadSchema,
      subscribe: () => connection.subscribe({ event: event.name }),
    }));
    return {
      name: Schema.decodeUnknownSync(SourceName)("lark"),
      tools: { get: makeGetTool(api) },
      events,
      instructions: "Lark: read with lark_get.",
    };
  });

export * as Lark from "./source.ts";
