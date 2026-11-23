import { Effect, Option, Schema } from "effect";
import {
  type EventType,
  type Event,
  type Source,
  type SourceError,
  SourceName,
  Rejected,
} from "../source/contract.ts";
import generated from "./events.json";
import { GitlabSocket, type ChannelIdentifier } from "./socket.ts";
import { type GitlabEvent, decodeGitlabCatalog } from "./catalog.ts";
import { GitlabClient, makeGetTool } from "./client.ts";
import { GitlabAuth } from "./auth.ts";
import { makeNotesEvent } from "./notes.ts";
import { targetFor, type TargetSupport } from "./target.ts";
import { subscribeToChannel } from "./channel.ts";

const catalog = decodeGitlabCatalog(generated);

const GraphqlMessage = Schema.Struct({
  result: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        data: Schema.optionalKey(Schema.NullOr(Schema.JsonObject)),
        errors: Schema.optionalKey(
          Schema.NullOr(Schema.Array(Schema.Struct({ message: Schema.String }))),
        ),
      }),
    ),
  ),
});

const toEvent = (
  event: GitlabEvent,
  message: Schema.Json,
): Effect.Effect<Option.Option<Event>, SourceError> =>
  Effect.gen(function* () {
    const decoded = Schema.decodeUnknownOption(GraphqlMessage)(message);
    if (Option.isNone(decoded)) {
      yield* Effect.logWarning(`Ignoring a malformed GitLab message for ${event.name}`);
      return Option.none();
    }
    const result = decoded.value.result;
    if (result === null || result === undefined) return Option.none();
    const errors = result.errors ?? [];
    if (errors.length > 0)
      return yield* Effect.fail(
        new Rejected({ message: errors.map((error) => error.message).join("; ") }),
      );
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

const identifierFor = (event: GitlabEvent, variables: Schema.JsonObject): ChannelIdentifier => ({
  channel: "GraphqlChannel",
  query: event.query,
  variables,
  operationName: null,
  nonce: crypto.randomUUID(),
});

const graphqlEvent = (options: {
  readonly event: GitlabEvent;
  readonly socket: GitlabSocket.Interface;
  readonly target: TargetSupport | undefined;
  readonly variables: (params: Schema.JsonObject) => Effect.Effect<Schema.JsonObject, SourceError>;
}): EventType => ({
  name: options.event.name,
  description: options.event.description,
  payload: options.event.payload,
  target: options.target,
  subscribe: (params) =>
    Effect.gen(function* () {
      const variables = yield* options.variables(params);
      return yield* subscribeToChannel({
        socket: options.socket,
        identifier: identifierFor(options.event, variables),
        toEvent: (message) => toEvent(options.event, message),
      });
    }),
});

export const source: Effect.Effect<
  Source,
  never,
  GitlabAuth.Service | GitlabClient.Service | GitlabSocket.Service
> = Effect.gen(function* () {
  const identity = yield* GitlabAuth.Service;
  const client = yield* GitlabClient.Service;
  const socket = yield* GitlabSocket.Service;
  const tokenUser = identity.identity.pipe(
    Effect.map((user) => ({ userId: `gid://gitlab/User/${user.userId}` })),
  );
  const supportFor = (event: string) =>
    Effect.gen(function* () {
      const support = targetFor({ event, host: identity.host, client });
      if (support !== undefined) return support;
      if (event === "userMergeRequestUpdated") return undefined;
      return yield* Effect.die(new Error(`No target table entry for GitLab event ${event}`));
    });
  return {
    name: Schema.decodeUnknownSync(SourceName)("gitlab"),
    tools: { get: makeGetTool(client) },
    events: [
      ...(yield* Effect.forEach(catalog.events, (event) =>
        Effect.gen(function* () {
          return graphqlEvent({
            event,
            socket,
            target: yield* supportFor(event.name),
            // GitLab sends this event only to the token's user, so the server fills userId itself.
            variables:
              event.name === "userMergeRequestUpdated"
                ? () => tokenUser
                : (params) => Effect.succeed(params),
          });
        }),
      )),
      makeNotesEvent({ socket, target: yield* supportFor("noteable.notes_changed") }),
    ],
    instructions:
      "GitLab: every event except userMergeRequestUpdated takes a target, a link or shorthand such as group/project!123. The server reads the resource to check access. Read more with gitlab_get, before arming a watch or from its code.",
  };
});

export * as Gitlab from "./source.ts";
