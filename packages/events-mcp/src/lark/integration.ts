import { Effect, Schema } from "effect";
import { type EventKind, type Integration, IntegrationName } from "../core/integration.ts";
import { LarkAuth } from "./auth.ts";
import { LarkSocket } from "./socket.ts";
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

export const integration: Effect.Effect<Integration, never, LarkAuth.Service | LarkSocket.Service> =
  Effect.gen(function* () {
    const auth = yield* LarkAuth.Service;
    const socket = yield* LarkSocket.Service;
    const events: ReadonlyArray<EventKind> = catalog.events.map((event) => ({
      name: event.name,
      description: event.description ?? event.name,
      params: { type: "object", properties: {}, additionalProperties: false },
      payload: event.payloadSchema,
      subscribe: () => socket.subscribe({ event: event.name }),
    }));
    return {
      name: Schema.decodeUnknownSync(IntegrationName)("lark"),
      tools: { get: makeGetTool(auth) },
      events,
      instructions:
        "Lark: read with lark_get to find a chat, message, or user id before arming a watch, or from inside the script.",
    };
  });

export * as Lark from "./integration.ts";
