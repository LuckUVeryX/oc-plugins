import { Config, Duration, Effect, Exit, Option, Schema, Scope } from "effect";
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
import { globalIdHint, readNoteable } from "./noteable.ts";
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
  readonly cable?: Cable | undefined;
}

const cacheSuccess = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Effect.Effect<A, E, R>> =>
  Effect.cachedWithTTL(effect, (exit) =>
    Exit.isSuccess(exit) ? Duration.infinity : Duration.zero,
  );

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
    return Option.some({ payload: object.value });
  });

const identifierFor = (event: GitlabEvent, params: Schema.JsonObject): CableIdentifier => ({
  channel: "GraphqlChannel",
  query: event.query,
  variables: params,
  operationName: null,
  nonce: crypto.randomUUID(),
});

const databaseId = (globalId: string): string => globalId.slice(globalId.lastIndexOf("/") + 1);

const AccessParams = Schema.Struct({
  projectFullPath: Schema.optionalKey(Schema.String),
  projectId: Schema.optionalKey(Schema.String),
  userId: Schema.optionalKey(Schema.String),
  issuableId: Schema.optionalKey(Schema.String),
});

const checkProject = (
  client: GitlabClient,
  project: { readonly fullPath: string } | { readonly id: string },
): Effect.Effect<void, SourceError> => {
  const label = "fullPath" in project ? project.fullPath : project.id;
  const path =
    "fullPath" in project
      ? `/projects/${encodeURIComponent(project.fullPath)}`
      : `/projects/${databaseId(project.id)}`;
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
  readonly cable: Cable;
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
    const cable = options.cable ?? (yield* gitlabCable({ host, identity: identityEffect }));
    const checkAccess: (params: Schema.JsonObject) => Effect.Effect<void, SourceError> = Effect.fn(
      "GitlabSource.checkAccess",
    )(function* (params) {
      const access = yield* Schema.decodeUnknownEffect(AccessParams)(params).pipe(
        Effect.mapError((error) => SourceError.Rejected({ message: error.message })),
      );
      const identity = yield* identityEffect;
      if (access.projectFullPath !== undefined)
        return yield* checkProject(client, { fullPath: access.projectFullPath });
      if (access.projectId !== undefined)
        return yield* checkProject(client, { id: access.projectId });
      if (access.userId !== undefined) return yield* checkUser(identity, access.userId);
      if (access.issuableId !== undefined)
        return yield* checkMergeRequest(client, access.issuableId);
    });
    return {
      name: Schema.decodeUnknownSync(SourceName)("gitlab"),
      tools: { get: makeGetTool(client) },
      events: [
        ...catalog.events.map((event) =>
          graphqlEvent({
            event,
            cable,
            checkAccess,
          }),
        ),
        makeNotesEvent({ client, cable }),
      ],
      instructions:
        "GitLab: read with gitlab_get. IDs are global (gid://gitlab/MergeRequest/1234); notes_changed takes database ids.",
    };
  });
