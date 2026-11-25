import { Config, Context, Effect, Layer, Option, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import {
  Rejected,
  type SourceError,
  type Tool,
  ToolError,
  Unavailable,
} from "../source/contract.ts";
import { GetInput, makeTool, validPath } from "../source/tool.ts";

const missing = new Rejected({
  message: "No Codemagic API token. Set CODEMAGIC_TOKEN, then arm a new watch.",
});

export const token: Effect.Effect<string, SourceError> = Config.Redacted("CODEMAGIC_TOKEN").pipe(
  Effect.map((value) => Redacted.value(value).trim()),
  Effect.mapError(() => missing),
  Effect.filterOrFail(
    (value) => value !== "",
    () => missing,
  ),
);

export interface Interface {
  /** Reads a path under /api/v3. None when it does not exist or the token cannot read it. */
  readonly read: (
    path: string,
    query?: GetInput["query"],
  ) => Effect.Effect<Option.Option<Schema.JsonObject>, SourceError>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/CodemagicClient") {}

export const makeClient = (options: {
  readonly token: Effect.Effect<string, SourceError>;
  readonly http: HttpClient.HttpClient;
}): Interface => {
  const send = Effect.fnUntraced(function* (
    request: HttpClientRequest.HttpClientRequest,
    label: string,
  ) {
    const response = yield* options.http
      .execute(HttpClientRequest.setHeader(request, "x-auth-token", yield* options.token))
      .pipe(
        Effect.mapError(
          () => new Unavailable({ message: `Could not reach Codemagic for ${label}` }),
        ),
      );
    const { status } = response;
    if ((status >= 200 && status < 300) || status === 404) return response;
    const body = yield* response.text.pipe(Effect.orElseSucceed(() => ""));
    const message = `Codemagic HTTP ${status} on ${label}: ${body.slice(0, 300)}`;
    if (status === 401)
      return yield* Effect.fail(new Rejected({ message: `${message}. Check CODEMAGIC_TOKEN.` }));
    return yield* Effect.fail(
      status === 408 || status === 429 || status >= 500
        ? new Unavailable({ message })
        : new Rejected({ message }),
    );
  });

  const read = Effect.fn("CodemagicClient.read")(function* (
    path: string,
    query: GetInput["query"] = {},
  ) {
    const url = `https://codemagic.io/api/v3${path}`;
    const response = yield* send(HttpClientRequest.get(url, { urlParams: query }), path);
    // Codemagic answers a missing resource with its web app's HTML page and status 200.
    if (response.status === 404 || !response.headers["content-type"]?.includes("json"))
      return Option.none();
    return yield* response.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Schema.JsonObject)),
      Effect.mapBoth({
        onSuccess: Option.some,
        onFailure: () =>
          new Unavailable({ message: `Codemagic returned an unexpected response for ${path}` }),
      }),
    );
  });

  return { read };
};

export const layer = Layer.effect(
  Service,
  HttpClient.HttpClient.pipe(
    Effect.provide(FetchHttpClient.layer),
    // A redirect could carry the token to another host.
    Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
    Effect.map((http) => Service.of(makeClient({ token, http }))),
  ),
);

export const makeGetTool = (client: Interface): Tool =>
  makeTool({
    description:
      "Read Codemagic REST data with the token's permissions, for example /builds/<buildId>/actions to find a failed step. path starts with /, stays under /api/v3, and carries no query string or percent escapes. Pass parameters in query. Returns the JSON body.",
    input: GetInput,
    run: ({ path, query }) =>
      (validPath(path)
        ? client.read(path, query)
        : Effect.fail(new Rejected({ message: "Invalid Codemagic GET path" }))
      ).pipe(
        Effect.mapError(({ message }) => new ToolError({ message })),
        Effect.flatMap(
          Option.match({
            onNone: () =>
              Effect.fail(
                new ToolError({
                  message: `${path} does not exist, or CODEMAGIC_TOKEN cannot read it`,
                }),
              ),
            onSome: Effect.succeed,
          }),
        ),
      ),
  });

export * as CodemagicClient from "./client.ts";
