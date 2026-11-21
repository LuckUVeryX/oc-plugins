import { Effect, Exit, Queue, Redacted, Schema, Scope, Stream } from "effect";
import { expect, test } from "vitest";
import type { Cable } from "../src/gitlab/cable.ts";
import type { GitlabClient } from "../src/gitlab/client.ts";
import { makeGitlabSource } from "../src/gitlab/source.ts";
import type { GitlabIdentity } from "../src/gitlab/token.ts";
import { Health, SourceError } from "../src/source.ts";
import { connect } from "./harness.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const client: GitlabClient = {
  get: () => Effect.succeed({ id: 599 }),
  graphql: () =>
    Effect.succeed({
      mergeRequest: { id: "gid://gitlab/MergeRequest/48090", iid: "32", projectId: 599 },
    }),
};

const cable: Cable = {
  subscribeUnique: () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<Schema.Json, SourceError>();
      return Stream.fromQueue(queue);
    }),
  health: Stream.succeed(Health.Live()),
};

const Events = Schema.Struct({
  events: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      inputSchema: Schema.Struct({
        properties: Schema.Struct({
          params: Schema.Struct({
            properties: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
          }),
        }),
      }),
    }),
  ),
});

test("advertises a param that a real watch accepts", async () => {
  const scope = await Effect.runPromise(Scope.make());
  try {
    const source = await Effect.runPromise(
      makeGitlabSource({
        host: "gitlab.test",
        identity: Effect.succeed(identity),
        client,
        cable,
      }).pipe(Effect.provideService(Scope.Scope, scope)),
    );
    const h = await connect({ sources: [source] });
    try {
      const listed = await h.client.request(
        { method: "events/list", params: {} },
        Schema.toStandardSchemaV1(Events),
      );
      const event = listed.events.find((item) => item.name === "gitlab.ciPipelineStatusesUpdated");
      expect(event?.inputSchema.properties.params.properties).toHaveProperty("projectFullPath");

      const watch = (watchId: string, params: Record<string, unknown>) =>
        h.stream(
          {
            watchId,
            params,
            prepare: "return null",
            transform: "return null",
            expiresInSeconds: 1,
          },
          "gitlab.ciPipelineStatusesUpdated",
        ).done;

      expect(await watch("accepted", { projectFullPath: "glints/example" })).toEqual({});
      expect(await watch("rejected", {})).toMatchObject({ code: -32602 });
    } finally {
      await h.close();
    }
  } finally {
    await Effect.runPromise(Scope.close(scope, Exit.void));
  }
});
