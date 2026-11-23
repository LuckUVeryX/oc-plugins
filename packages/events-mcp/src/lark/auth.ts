import { Client, Domain } from "@larksuiteoapi/node-sdk";
import { Config, Context, Effect, Layer, Redacted } from "effect";
import { Rejected, type SourceError } from "../source/contract.ts";
import type { GetInput } from "../source/tool.ts";
import type { LarkRequester } from "./get.ts";
import { sdkLogger } from "./sdk-logger.ts";

interface Credentials {
  readonly appId: string;
  readonly appSecret: string;
  readonly request: LarkRequester;
}

export interface Interface {
  readonly credentials: () => Effect.Effect<Credentials, SourceError>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/LarkAuth") {}

const credentialMessage =
  "No Lark app credentials. Set LARK_APP_ID to cli_ followed by 16 hex characters, and LARK_APP_SECRET to the app secret.";

const rejected = new Rejected({ message: credentialMessage });

const credentials = Effect.fn("LarkAuth.credentials")(function* () {
  const appId = yield* Config.String("LARK_APP_ID").pipe(Effect.mapError(() => rejected));
  const secret = yield* Config.Redacted("LARK_APP_SECRET").pipe(Effect.mapError(() => rejected));
  const id = appId.trim();
  const key = Redacted.value(secret).trim();
  if (!/^cli_[0-9a-fA-F]{16}$/.test(id) || key === "") return yield* Effect.fail(rejected);
  const client = new Client({
    appId: id,
    appSecret: key,
    domain: Domain.Lark,
    logger: sdkLogger,
  });
  return {
    appId: id,
    appSecret: key,
    request: (input: GetInput, signal: AbortSignal) =>
      client.request<unknown>({
        method: "GET",
        url: `https://open.larksuite.com/open-apis${input.path}`,
        params: input.query,
        signal,
      }),
  };
});

export const layer = Layer.succeed(Service, Service.of({ credentials }));

export * as LarkAuth from "./auth.ts";
