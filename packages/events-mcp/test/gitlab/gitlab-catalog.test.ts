import { Effect, Layer, Queue, Redacted, Schema, Stream } from "effect";
import { expect, test } from "vitest";
import { GitlabSocket } from "../../src/gitlab/socket.ts";
import { GitlabClient } from "../../src/gitlab/client.ts";
import { GitlabAuth } from "../../src/gitlab/auth.ts";
import { Gitlab } from "../../src/gitlab/integration.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { Health, type IntegrationError } from "../../src/core/integration.ts";
import { connect } from "../harness.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const client: GitlabClient.Interface = {
  get: () => Effect.succeed({ id: 599 }),
  graphql: () =>
    Effect.succeed({
      mergeRequest: { id: "gid://gitlab/MergeRequest/48090", iid: "32", projectId: 599 },
    }),
};

const socket: GitlabSocket.Interface = {
  subscribe: () =>
    Effect.gen(function* () {
      const queue = yield* Queue.unbounded<Schema.Json, IntegrationError>();
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
  const integration = await Effect.runPromise(
    Gitlab.integration.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(
            GitlabAuth.Service,
            GitlabAuth.Service.of({ host: "gitlab.test", identity: Effect.succeed(identity) }),
          ),
          Layer.succeed(GitlabClient.Service, GitlabClient.Service.of(client)),
          Layer.succeed(GitlabSocket.Service, GitlabSocket.Service.of(socket)),
        ),
      ),
    ),
  );
  const h = await connect({ integrations: [integration] });
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

    expect(await watch("accepted", { projectFullPath: "group/project" })).toEqual({});
    expect(await watch("rejected", {})).toMatchObject({ code: -32602 });
  } finally {
    await h.close();
  }
});
