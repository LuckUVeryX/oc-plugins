import { Effect, Option, Schema } from "effect";
import { HttpClientRequest, HttpClientResponse } from "effect/http";
import { type Tool, ToolError } from "../source/contract.ts";
import { GetInput, makeTool, validPath } from "../source/tool.ts";
import type { CodemagicClient } from "./client.ts";

const Id = Schema.String.check(Schema.isPattern(/^[0-9a-f]{24}$/));

const fail = (message: string) => Effect.fail(new ToolError({ message }));

const toToolError = Effect.mapError(
  ({ message }: { readonly message: string }) => new ToolError({ message }),
);

export const makeGetTool = (client: CodemagicClient.Interface): Tool =>
  makeTool({
    description:
      "Read Codemagic REST data with the token's permissions, for example /builds/<buildId>/actions to find a failed step. path starts with /, stays under /api/v3, and carries no query string or percent escapes. Pass parameters in query. Returns the JSON body.",
    input: GetInput,
    run: ({ path, query }) =>
      Effect.gen(function* () {
        if (!validPath(path)) return yield* fail("Invalid Codemagic GET path");
        const found = yield* client.read(path, query);
        if (Option.isNone(found))
          return yield* fail(`${path} does not exist, or CODEMAGIC_TOKEN cannot read it`);
        return found.value;
      }).pipe(toToolError),
  });

const Started = Schema.Struct({ data: Schema.Struct({ id: Schema.String }) });

export const makeStartTool = (client: CodemagicClient.Interface): Tool =>
  makeTool({
    description:
      "Start a Codemagic build. Pass exactly one of branch and tag. Returns the buildId once Codemagic accepts the request. After an error that leaves the outcome unclear, check the app's builds before trying again.",
    input: Schema.Struct({
      appId: Id,
      workflowId: Schema.NonEmptyString,
      branch: Schema.optionalKey(Schema.NonEmptyString),
      tag: Schema.optionalKey(Schema.NonEmptyString),
      labels: Schema.optionalKey(Schema.Array(Schema.NonEmptyString)),
    }),
    run: ({ appId, workflowId, ...rest }) =>
      Effect.gen(function* () {
        const response = yield* client.send(
          HttpClientRequest.post(`https://codemagic.io/api/v3/apps/${appId}/builds`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ workflow_id: workflowId, ...rest }),
          ),
          "the build start",
        );
        if (response.status !== 202)
          return yield* fail(`App ${appId} does not exist, or CODEMAGIC_TOKEN cannot build it`);
        const { data } = yield* HttpClientResponse.schemaBodyJson(Started)(response);
        return { buildId: data.id };
      }).pipe(toToolError),
  });

export const makeCancelTool = (client: CodemagicClient.Interface): Tool =>
  makeTool({
    description: "Cancel a Codemagic build. canceled is false when the build had already ended.",
    input: Schema.Struct({ buildId: Id }),
    run: ({ buildId }) =>
      Effect.gen(function* () {
        const { status } = yield* client.send(
          HttpClientRequest.post(`https://codemagic.io/api/v3/builds/${buildId}/cancel`),
          `the cancel of build ${buildId}`,
        );
        if (status !== 204 && status !== 409)
          return yield* fail(
            `Build ${buildId} does not exist, or CODEMAGIC_TOKEN cannot cancel it`,
          );
        return { buildId, canceled: status === 204 };
      }).pipe(toToolError),
  });

// API v3 has no logs or public artifact links, so these come from the older API.
const LEGACY = "https://api.codemagic.io";

const LogUrl = Schema.optionalKey(Schema.NullOr(Schema.String));
const LegacyBuild = Schema.Struct({
  build: Schema.Struct({
    finishedAt: Schema.optionalKey(Schema.NullOr(Schema.String)),
    buildActions: Schema.Array(
      Schema.Struct({
        _id: Schema.String,
        name: Schema.String,
        status: Schema.optionalKey(Schema.NullOr(Schema.String)),
        logUrl: LogUrl,
        subactions: Schema.optionalKey(
          Schema.NullOr(Schema.Array(Schema.Struct({ logUrl: LogUrl }))),
        ),
      }),
    ),
  }),
});

const readLegacyBuild = Effect.fnUntraced(function* (
  client: CodemagicClient.Interface,
  buildId: string,
) {
  const response = yield* client.send(
    HttpClientRequest.get(`${LEGACY}/builds/${buildId}`),
    `build ${buildId}`,
  );
  if (response.status !== 200)
    return yield* fail(`Build ${buildId} does not exist, or CODEMAGIC_TOKEN cannot read it`);
  return (yield* HttpClientResponse.schemaBodyJson(LegacyBuild)(response)).build;
});

const MAX_LOG_CHARACTERS = 50_000;

export const makeLogsTool = (client: CodemagicClient.Interface): Tool =>
  makeTool({
    description:
      "Read the output of one Codemagic build step. Find stepId with codemagic_get at /builds/<buildId>/actions. Returns the end of the log, up to 50,000 characters. complete is false while the step runs or when part of its log is missing. Check the text for secrets before sharing it.",
    input: Schema.Struct({ buildId: Id, stepId: Id }),
    run: ({ buildId, stepId }) =>
      Effect.gen(function* () {
        const build = yield* readLegacyBuild(client, buildId);
        const step = build.buildActions.find((action) => action._id === stepId);
        if (step === undefined) return yield* fail(`Build ${buildId} has no step ${stepId}`);
        // A script step keeps its logs on its subactions. One without output has no log URL.
        const urls = [step, ...(step.subactions ?? [])].flatMap(({ logUrl }) => logUrl ?? []);
        const responses = yield* Effect.forEach(urls, (url) =>
          client.send(
            HttpClientRequest.get(
              `${LEGACY}/builds/${buildId}/step/${encodeURIComponent(url.slice(url.lastIndexOf("/") + 1))}`,
            ),
            `the log of step ${stepId}`,
          ),
        );
        // A step that never ran has no status, even after its build ended.
        const ended = typeof (step.status ?? build.finishedAt) === "string";
        const ready = responses.filter(({ status }) => status === 200);
        const log = (yield* Effect.forEach(ready, ({ text }) => text)).join("\n");
        return {
          step: step.name,
          complete: ended && ready.length === urls.length,
          log: log.slice(-MAX_LOG_CHARACTERS),
          truncated: log.length > MAX_LOG_CHARACTERS,
        };
      }).pipe(toToolError),
  });
