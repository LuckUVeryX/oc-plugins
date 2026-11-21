import { Client, Domain } from "@larksuiteoapi/node-sdk";
import { Config, Effect, Redacted, Schema, type Scope } from "effect";
import generated from "../generated/events.json";
import { type EventType, type Source, SourceError, SourceName } from "../source.ts";
import { sdkLogger } from "../stderr-logger.ts";
import { makeGetTool, type LarkRequester } from "./get.ts";
import { makeConnection } from "./producers.ts";

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

export interface LarkOptions {
  readonly request?: LarkRequester;
}

const credentialMessage =
  "No Lark app credentials. Set LARK_APP_ID to cli_ followed by 16 hex characters, and LARK_APP_SECRET to the app secret.";

const rejected = SourceError.Rejected({ message: credentialMessage });

const credentials = Effect.fn("Lark.credentials")(function* (request: LarkRequester | undefined) {
  const appId = yield* Config.String("LARK_APP_ID").pipe(Effect.mapError(() => rejected));
  const secret = yield* Config.Redacted("LARK_APP_SECRET").pipe(Effect.mapError(() => rejected));
  const id = appId.trim();
  const key = Redacted.value(secret).trim();
  if (!/^cli_[0-9a-fA-F]{16}$/.test(id) || key === "") return yield* Effect.fail(rejected);
  if (request !== undefined) return { appId: id, appSecret: key, request };
  const client = new Client({
    appId: id,
    appSecret: key,
    domain: Domain.Lark,
    logger: sdkLogger,
  });
  return {
    appId: id,
    appSecret: key,
    request: (input, signal) =>
      client.request<unknown>({
        method: "GET",
        url: `https://open.larksuite.com/open-apis${input.path}`,
        params: input.query,
        signal,
      }),
  };
});

export const makeLarkSource = (
  options: LarkOptions = {},
): Effect.Effect<Source, never, Scope.Scope> =>
  Effect.gen(function* () {
    const app = credentials(options.request);
    const connection = yield* makeConnection({ credentials: app });
    const events: ReadonlyArray<EventType> = catalog.events.map((event) => ({
      name: event.name,
      description: event.description ?? event.name,
      params: { type: "object", properties: {}, additionalProperties: false },
      payload: event.payloadSchema,
      subscribe: () => connection.subscribe({ event: event.name }),
    }));
    return {
      name: Schema.decodeUnknownSync(SourceName)("lark"),
      tools: { get: makeGetTool(app.pipe(Effect.map((value) => value.request))) },
      events,
      instructions: "Lark: read with lark_get.",
    };
  });
