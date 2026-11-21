import { Effect, Option, Schema } from "effect";
import {
  type EventKind,
  type Event,
  Rejected,
  type Integration,
  type IntegrationError,
  IntegrationName,
} from "../core/integration.ts";
import generated from "./events.json";
import { GitlabSocket, type ChannelIdentifier } from "./socket.ts";
import { type GitlabEvent, decodeGitlabCatalog } from "./catalog.ts";
import { GitlabClient, GraphqlResult, graphqlFailure, makeGetTool } from "./client.ts";
import { GitlabAuth } from "./auth.ts";
import { globalIdHint, readNoteable } from "./noteable.ts";
import { makeNotesEvent } from "./notes.ts";
import { subscribeToChannel } from "./channel.ts";
import type { GitlabIdentity as TokenIdentity } from "./token.ts";

const catalog = decodeGitlabCatalog(generated);

const GraphqlMessage = Schema.Struct({
  result: Schema.optionalKey(Schema.NullOr(GraphqlResult)),
});

const toEvent = (
  event: GitlabEvent,
  message: Schema.Json,
): Effect.Effect<Option.Option<Event>, IntegrationError> =>
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

const identifierFor = (event: GitlabEvent, params: Schema.JsonObject): ChannelIdentifier => ({
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
  client: GitlabClient.Interface,
  project: { readonly fullPath: string } | { readonly id: string },
): Effect.Effect<void, IntegrationError> => {
  const label = "fullPath" in project ? project.fullPath : project.id;
  const path =
    "fullPath" in project
      ? `/projects/${encodeURIComponent(project.fullPath)}`
      : `/projects/${databaseId(project.id)}`;
  return client.get({ path }).pipe(
    Effect.catchTag("Integration.Rejected", (error) =>
      Effect.fail(
        new Rejected({
          message: `Project ${label} is not accessible with this GitLab token: ${error.message}`,
        }),
      ),
    ),
  );
};

const checkUser = (
  identity: TokenIdentity,
  userId: string,
): Effect.Effect<void, IntegrationError> => {
  if (databaseId(userId) === String(identity.userId)) return Effect.void;
  return Effect.fail(
    new Rejected({
      message: `userId ${userId} is not the token's user, @${identity.username} (gid://gitlab/User/${identity.userId}). GitLab only delivers merge requests assigned to or reviewed by the token's user.`,
    }),
  );
};

const graphqlEvent = (options: {
  readonly event: GitlabEvent;
  readonly socket: GitlabSocket.Interface;
  readonly checkAccess: (params: Schema.JsonObject) => Effect.Effect<void, IntegrationError>;
}): EventKind => ({
  name: options.event.name,
  description: options.event.description,
  params: options.event.params,
  payload: options.event.payload,
  subscribe: (params) =>
    Effect.gen(function* () {
      yield* options.checkAccess(params);
      return yield* subscribeToChannel({
        socket: options.socket,
        identifier: identifierFor(options.event, params),
        toEvent: (message) => toEvent(options.event, message),
      });
    }),
});

export const integration: Effect.Effect<
  Integration,
  never,
  GitlabAuth.Service | GitlabClient.Service | GitlabSocket.Service
> = Effect.gen(function* () {
  const identity = yield* GitlabAuth.Service;
  const client = yield* GitlabClient.Service;
  const socket = yield* GitlabSocket.Service;
  const checkAccess = Effect.fn("Gitlab.checkAccess")(function* (params: Schema.JsonObject) {
    const access = yield* Schema.decodeUnknownEffect(AccessParams)(params).pipe(
      Effect.mapError((error) => new Rejected({ message: error.message })),
    );
    const token = yield* identity.identity;
    if (access.projectFullPath !== undefined)
      return yield* checkProject(client, { fullPath: access.projectFullPath });
    if (access.projectId !== undefined)
      return yield* checkProject(client, { id: access.projectId });
    if (access.userId !== undefined) return yield* checkUser(token, access.userId);
    if (access.issuableId !== undefined)
      return yield* readNoteable(
        client,
        "merge_request",
        access.issuableId,
        globalIdHint("merge_request"),
      ).pipe(Effect.asVoid);
  });
  return {
    name: Schema.decodeUnknownSync(IntegrationName)("gitlab"),
    tools: { get: makeGetTool(client) },
    events: [
      ...catalog.events.map((event) =>
        graphqlEvent({
          event,
          socket,
          checkAccess,
        }),
      ),
      makeNotesEvent({ client, socket }),
    ],
    instructions:
      "GitLab: read with gitlab_get. IDs are global (gid://gitlab/MergeRequest/1234); notes_changed takes database ids.",
  };
});

export * as Gitlab from "./integration.ts";
