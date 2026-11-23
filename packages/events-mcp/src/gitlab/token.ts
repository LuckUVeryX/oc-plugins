import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { Config, Effect, Option, Redacted, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
import { Rejected, type SourceError, Unavailable } from "../source/contract.ts";

export interface GitlabIdentity {
  readonly token: Redacted.Redacted<string>;
  readonly userId: number;
  readonly username: string;
}

const missing = (host: string): SourceError =>
  new Rejected({
    message: `No GitLab token for ${host}. Set GITLAB_TOKEN, or authenticate glab so \`glab config get token --host ${host}\` returns one.`,
  });

const exec = promisify(execFile);

const fromGlab = (host: string): Effect.Effect<Redacted.Redacted<string>, SourceError> =>
  Effect.tryPromise({
    try: async () => {
      const { stdout } = await exec("glab", ["config", "get", "token", "--host", host], {
        encoding: "utf8",
      });
      return stdout.trim();
    },
    catch: () => missing(host),
  }).pipe(
    Effect.filterOrFail(
      (token) => token.length > 0,
      () => missing(host),
    ),
    Effect.map(Redacted.make),
  );

const UserResponse = Schema.Struct({
  id: Schema.Number,
  username: Schema.String,
});

const validate = Effect.fn("GitlabToken.validate")(function* (input: {
  readonly host: string;
  readonly token: Redacted.Redacted<string>;
}) {
  const request = HttpClientRequest.get(`https://${input.host}/api/v4/user`).pipe(
    HttpClientRequest.bearerToken(Redacted.value(input.token)),
    HttpClientRequest.acceptJson,
  );
  const response = yield* HttpClient.execute(request).pipe(
    Effect.mapError(
      () =>
        new Unavailable({
          message: `Could not reach https://${input.host}/api/v4/user to validate the GitLab token`,
        }),
    ),
  );
  if (response.status !== 200)
    return yield* Effect.fail(
      response.status === 401 || response.status === 403
        ? new Rejected({
            message: `GitLab rejected the token for ${input.host} (HTTP ${response.status})`,
          })
        : new Unavailable({
            message: `GitLab could not validate the token for ${input.host} (HTTP ${response.status})`,
          }),
    );
  const user = yield* HttpClientResponse.schemaBodyJson(UserResponse)(response).pipe(
    Effect.mapError(
      () =>
        new Unavailable({
          message: `GitLab returned an unexpected user response for ${input.host}`,
        }),
    ),
  );
  return { token: input.token, userId: user.id, username: user.username };
});

export const resolveIdentity = Effect.fn("GitlabToken.resolve")(function* (host: string) {
  const configured = yield* Config.Redacted("GITLAB_TOKEN").pipe(
    Config.option,
    Effect.orElseSucceed(() => Option.none()),
  );
  const value = Option.getOrUndefined(configured);
  const token =
    value === undefined || Redacted.value(value).trim().length === 0
      ? yield* fromGlab(host)
      : Redacted.make(Redacted.value(value).trim());
  return yield* validate({ host, token });
});
