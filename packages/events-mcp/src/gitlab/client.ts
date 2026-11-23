import { Context, Effect, Layer, Option, Redacted, Result, Schema } from "effect";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
} from "effect/http";
import {
  Rejected,
  type IntegrationError,
  ToolError,
  type Tool,
  Unavailable,
} from "../integration/contract.ts";
import { GetInput, makeTool } from "../integration/tool.ts";
import { GitlabAuth } from "./auth.ts";
import type { GitlabIdentity } from "./token.ts";

export interface Interface {
  readonly get: (input: GetInput) => Effect.Effect<Schema.Json, IntegrationError>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/GitlabClient") {}

interface GitlabClientOptions {
  readonly host: string;
  readonly identity: Effect.Effect<GitlabIdentity, IntegrationError>;
  readonly http?: HttpClient.HttpClient | undefined;
}

const ErrorBody = Schema.Union([
  Schema.Struct({ message: Schema.String }),
  Schema.Struct({ error: Schema.String }),
]);

const invalidPath = (reason: string): IntegrationError =>
  new Rejected({ message: `Invalid GitLab REST path. ${reason}` });

const validPath = (path: string): boolean =>
  path.startsWith("/") && !path.includes("//") && !path.includes("..") && !/[?#\\\s]/.test(path);

const errorBody = (body: string): string => {
  const trimmed = body.trim();
  const parsed = Result.getOrUndefined(Result.try(() => JSON.parse(trimmed)));
  return Option.match(Schema.decodeUnknownOption(ErrorBody)(parsed), {
    onNone: () => trimmed.slice(0, 300),
    onSome: (value) => ("message" in value ? value.message : value.error),
  });
};

const statusError = (status: number, label: string, body: string): IntegrationError => {
  const detail = errorBody(body);
  const message = `GitLab HTTP ${status} on ${label}${detail === "" ? "" : `: ${detail}`}`;
  return status === 408 || status === 429 || status >= 500
    ? new Unavailable({ message })
    : new Rejected({ message });
};

const jsonBody = (response: HttpClientResponse.HttpClientResponse, label: string) =>
  response.json.pipe(
    Effect.mapError(
      () => new Unavailable({ message: `Could not read the GitLab response from ${label}` }),
    ),
    // response.json parses JSON numbers such as 1e400 to Infinity, which no MCP client can read back.
    Effect.flatMap((value) =>
      Schema.decodeUnknownEffect(Schema.Json)(value).pipe(
        Effect.mapError(
          () => new Unavailable({ message: `GitLab returned invalid JSON from ${label}` }),
        ),
      ),
    ),
  );

export const makeGitlabClient = (options: GitlabClientOptions): Effect.Effect<Interface> =>
  Effect.gen(function* () {
    const base = new URL(`https://${options.host}`);
    const http =
      options.http ??
      (yield* HttpClient.HttpClient.pipe(
        Effect.provide(FetchHttpClient.layer),
        // A redirect could leave the host and the /api/v4 prefix, so let 3xx responses through as errors.
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
      ));

    const authorize = options.identity;

    const run = (
      request: HttpClientRequest.HttpClientRequest,
      label: string,
    ): Effect.Effect<Schema.Json, IntegrationError> =>
      Effect.gen(function* () {
        const response = yield* http
          .execute(request)
          .pipe(
            Effect.mapError(
              () => new Unavailable({ message: `Could not reach GitLab on ${label}` }),
            ),
          );
        if (response.status < 200 || response.status >= 300) {
          const body = yield* response.text.pipe(Effect.orElseSucceed(() => ""));
          return yield* Effect.fail(statusError(response.status, label, body));
        }
        return yield* jsonBody(response, label);
      });

    const get = Effect.fn("GitlabClient.get")(function* (input: GetInput) {
      if (!validPath(input.path))
        return yield* Effect.fail(
          invalidPath(
            "Use a path relative to /api/v4, such as /projects/group%2Fproject/pipelines, without .., //, a query string or a fragment.",
          ),
        );
      const identity = yield* authorize;
      const url = new URL(`/api/v4${input.path}`, base);
      for (const [key, value] of Object.entries(input.query ?? {}))
        url.searchParams.set(key, String(value));
      // The URL parser decodes percent-encoded dot segments, so re-check the path after normalisation.
      if (url.origin !== base.origin || !url.pathname.startsWith("/api/v4/"))
        return yield* Effect.fail(
          invalidPath(
            `It must stay under https://${options.host}/api/v4, without .., //, a query string or a fragment.`,
          ),
        );
      const request = HttpClientRequest.get(url.toString()).pipe(
        HttpClientRequest.bearerToken(Redacted.value(identity.token)),
        HttpClientRequest.acceptJson,
      );
      return yield* run(request, input.path);
    });

    return { get };
  });

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const identity = yield* GitlabAuth.Service;
    return Service.of(
      yield* makeGitlabClient({ host: identity.host, identity: identity.identity }),
    );
  }),
);

export const makeGetTool = (client: Interface): Tool =>
  makeTool({
    description:
      "Read GitLab REST data with the token's permissions, for example to find the database id behind a merge request or to read notes before arming a watch or from inside the script. path starts with /, stays under /api/v4, and carries no query string. Pass parameters in query. Percent-encode a project path as /projects/group%2Fproject. Returns the JSON body.",
    input: GetInput,
    run: (input) =>
      client.get(input).pipe(Effect.mapError((error) => new ToolError({ message: error.message }))),
  });

export * as GitlabClient from "./client.ts";
