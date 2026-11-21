import { Config, DateTime, Duration, Effect, Exit, Option, Schema, Scope } from "effect";
import generated from "../generated/gitlab-events.json";
import {
  type EventType,
  type Occurrence,
  type Source,
  SourceError,
  SourceName,
} from "../source.ts";
import { type Cable, type CableIdentifier, gitlabCable } from "./cable.ts";
import { type GitlabEvent, decodeGitlabCatalog } from "./catalog.ts";
import {
  type GitlabClient,
  GraphqlResult,
  graphqlFailure,
  makeGetTool,
  makeGitlabClient,
} from "./client.ts";
import { globalIdHint, mergeRequestGlobalId, readNoteable } from "./noteable.ts";
import { makeNotesEvent } from "./notes.ts";
import { subscribeToChannel } from "./subscription.ts";
import { type GitlabIdentity, resolveIdentity } from "./token.ts";

const catalog = decodeGitlabCatalog(generated);

const GraphqlMessage = Schema.Struct({
  result: Schema.optionalKey(Schema.NullOr(GraphqlResult)),
});

export interface GitlabOptions {
  readonly host?: string | undefined;
  readonly identity?: Effect.Effect<GitlabIdentity, SourceError> | undefined;
  readonly client?: GitlabClient | undefined;
  readonly cable?: Effect.Effect<Cable, SourceError, Scope.Scope> | undefined;
}

const cacheSuccess = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Effect.Effect<A, E, R>> =>
  Effect.cachedWithTTL(effect, (exit) =>
    Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
  );

const occurredAt = (event: GitlabEvent, payload: Schema.JsonObject): DateTime.Utc | undefined => {
  if (event.occurredAtField === undefined) return undefined;
  const value = payload[event.occurredAtField];
  return typeof value === "string" ? Option.getOrUndefined(DateTime.make(value)) : undefined;
};

const toOccurrence = (
  event: GitlabEvent,
  message: Schema.Json,
): Effect.Effect<Option.Option<Occurrence>, SourceError> =>
  Effect.gen(function* () {
    const decoded = Schema.decodeUnknownOption(GraphqlMessage)(message);
    if (Option.isNone(decoded)) {
      yield* Effect.logWarning(`Ignoring a malformed GitLab message for ${event.name}`);
      return Option.none();
    }
    const result = decoded.value.result;
    if (result === null || result === undefined) return Option.none();
    const errors = result.errors ?? [];
    if (errors.length > 0) return yield* Effect.fail(graphqlFailure(errors));
    const data = result.data;
    if (data === null || data === undefined) return Option.none();
    const payload = data[event.name];
    const object = Schema.decodeUnknownOption(Schema.JsonObject)(payload);
    if (Option.isNone(object)) {
      if (payload !== null && payload !== undefined)
        yield* Effect.logWarning(`Ignoring a non-object GitLab payload for ${event.name}`);
      return Option.none();
    }
    const time = occurredAt(event, object.value);
    return Option.some({
      ...(time === undefined ? {} : { occurredAt: time }),
      payload: object.value,
    });
  });

const identifierFor = (event: GitlabEvent, params: Schema.JsonObject): CableIdentifier => ({
  channel: "GraphqlChannel",
  query: event.query,
  variables: params,
  operationName: null,
  nonce: crypto.randomUUID(),
});

const databaseId = (globalId: string): string => globalId.slice(globalId.lastIndexOf("/") + 1);

const GLOBAL_ID_PREFIXES: Readonly<Record<string, string>> = {
  projectId: "gid://gitlab/Project/",
  userId: "gid://gitlab/User/",
  issuableId: mergeRequestGlobalId,
  pipelineId: "gid://gitlab/Ci::Pipeline/",
  jobId: "gid://gitlab/Ci::Build/",
};

const invalidGlobalId = (name: string, value: unknown, prefix: string): string | undefined =>
  typeof value === "string" && value.startsWith(prefix) && /^\d+$/.test(value.slice(prefix.length))
    ? undefined
    : `${name} must be a GitLab global ID such as ${prefix}123, not the number in the web URL.`;

const checkProject = (
  client: GitlabClient,
  params: Schema.JsonObject,
): Effect.Effect<void, SourceError> => {
  const fullPath = params["projectFullPath"];
  const projectId = params["projectId"];
  if (typeof fullPath !== "string" && typeof projectId !== "string")
    return Effect.fail(SourceError.Rejected({ message: "Pass projectFullPath or projectId." }));
  const label = typeof fullPath === "string" ? fullPath : String(projectId);
  const path =
    typeof fullPath === "string"
      ? `/projects/${encodeURIComponent(fullPath)}`
      : `/projects/${databaseId(String(projectId))}`;
  return client.get({ path }).pipe(
    Effect.catchTag("Rejected", (error) =>
      Effect.fail(
        SourceError.Rejected({
          message: `Project ${label} is not accessible with this GitLab token: ${error.message}`,
        }),
      ),
    ),
  );
};

const checkUser = (identity: GitlabIdentity, userId: string): Effect.Effect<void, SourceError> => {
  if (databaseId(userId) === String(identity.userId)) return Effect.void;
  return Effect.fail(
    SourceError.Rejected({
      message: `userId ${userId} is not the token's user, @${identity.username} (gid://gitlab/User/${identity.userId}). GitLab only delivers merge requests assigned to or reviewed by the token's user.`,
    }),
  );
};

const checkMergeRequest = (
  client: GitlabClient,
  issuableId: string,
): Effect.Effect<void, SourceError> =>
  readNoteable(client, "merge_request", issuableId, globalIdHint("merge_request")).pipe(
    Effect.asVoid,
  );

const graphqlEvent = (options: {
  readonly event: GitlabEvent;
  readonly cable: Effect.Effect<Cable, SourceError, Scope.Scope>;
  readonly checkAccess: (params: Schema.JsonObject) => Effect.Effect<void, SourceError>;
}): EventType => ({
  name: options.event.name,
  description: options.event.description,
  params: options.event.params,
  payload: options.event.payload,
  subscribe: (params) =>
    Effect.gen(function* () {
      yield* options.checkAccess(params);
      return yield* subscribeToChannel({
        cable: options.cable,
        identifier: identifierFor(options.event, params),
        toOccurrence: (message) => toOccurrence(options.event, message),
      });
    }),
});

export const makeGitlabSource = (
  options: GitlabOptions = {},
): Effect.Effect<Source, never, Scope.Scope> =>
  Effect.gen(function* () {
    const host =
      options.host ??
      (yield* Config.String("GITLAB_HOST").pipe(
        Config.withDefault("gitlab.example.com"),
        Effect.orDie,
      ));
    if (host.trim() === "") yield* Effect.die(new Error("GITLAB_HOST must not be empty"));
    const identityEffect = yield* cacheSuccess(options.identity ?? resolveIdentity(host));
    const client = options.client ?? (yield* makeGitlabClient({ host, identity: identityEffect }));
    const scope = yield* Effect.scope;
    const cableEffect: Effect.Effect<Cable, SourceError, Scope.Scope> = yield* cacheSuccess(
      Effect.gen(function* () {
        if (options.cable !== undefined) return yield* options.cable;
        const identity = yield* identityEffect;
        return yield* gitlabCable({ host, token: identity.token });
      }).pipe(Effect.provideService(Scope.Scope, scope)),
    );
    const checkAccess: (params: Schema.JsonObject) => Effect.Effect<void, SourceError> = Effect.fn(
      "GitlabSource.checkAccess",
    )(function* (params) {
      for (const [name, prefix] of Object.entries(GLOBAL_ID_PREFIXES)) {
        const value = params[name];
        if (value === undefined) continue;
        const message = invalidGlobalId(name, value, prefix);
        if (message !== undefined) return yield* Effect.fail(SourceError.Rejected({ message }));
      }
      const identity = yield* identityEffect;
      const userId = params["userId"];
      const issuableId = params["issuableId"];
      if (typeof params["projectFullPath"] === "string" || typeof params["projectId"] === "string")
        return yield* checkProject(client, params);
      if (typeof userId === "string") return yield* checkUser(identity, userId);
      if (typeof issuableId === "string") return yield* checkMergeRequest(client, issuableId);
    });
    return {
      name: Schema.decodeUnknownSync(SourceName)("gitlab"),
      tools: { get: makeGetTool(client) },
      events: [
        ...catalog.events.map((event) =>
          graphqlEvent({
            event,
            cable: cableEffect,
            checkAccess,
          }),
        ),
        makeNotesEvent({ client, cable: cableEffect }),
      ],
      instructions:
        "GitLab: read with gitlab_get. IDs are global (gid://gitlab/MergeRequest/1234); notes_changed takes database ids.",
    };
  });
