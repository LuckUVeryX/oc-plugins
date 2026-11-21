import { expect, test } from "vitest";
import { Effect, Fiber, Layer, Queue, Redacted, Schema, Scope, Stream } from "effect";
import { Cable } from "../../src/gitlab/cable.ts";
import type { GetInput } from "../../src/core/tool.ts";
import { GitlabClient, type GraphqlRequest } from "../../src/gitlab/client.ts";
import { GitlabIdentity } from "../../src/gitlab/identity.ts";
import { Gitlab } from "../../src/gitlab/source.ts";
import type { GitlabIdentity as TokenIdentity } from "../../src/gitlab/token.ts";
import { Health, Rejected, type SourceError, type Source } from "../../src/core/source.ts";
import { connect } from "../harness.ts";

const identity: TokenIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

interface FakeCable {
  readonly cable: Cable.Interface;
  readonly identifiers: Array<Schema.Json>;
  readonly queues: Array<Queue.Queue<Schema.Json, SourceError>>;
}

const makeFakeCable = (): FakeCable => {
  const identifiers: Array<Schema.Json> = [];
  const queues: Array<Queue.Queue<Schema.Json, SourceError>> = [];
  return {
    identifiers,
    queues,
    cable: {
      subscribeUnique: (identifier) =>
        Effect.gen(function* () {
          identifiers.push(identifier);
          const queue = yield* Queue.unbounded<Schema.Json, SourceError>();
          queues.push(queue);
          return Stream.fromQueue(queue);
        }),
      health: Stream.succeed(Health.Live()),
    },
  };
};

const makeFakeClient = (
  options: {
    readonly getError?: SourceError | undefined;
    readonly graphql?: ((id: Schema.Json) => Schema.JsonObject) | undefined;
  } = {},
) => {
  const gets: Array<GetInput> = [];
  const graphqls: Array<GraphqlRequest> = [];
  const client: GitlabClient.Interface = {
    get: (input) => {
      gets.push(input);
      return options.getError === undefined
        ? Effect.succeed({ id: 599 })
        : Effect.fail(options.getError);
    },
    graphql: (request) => {
      graphqls.push(request);
      return Effect.succeed(
        options.graphql?.(request.variables["id"] ?? null) ?? {
          mergeRequest: {
            id: "gid://gitlab/MergeRequest/48090",
            iid: "32",
            projectId: 599,
          },
          issue: {
            id: "gid://gitlab/Issue/1183",
            iid: "30",
            projectId: 18,
          },
        },
      );
    },
  };
  return { client, gets, graphqls };
};

const sourceWith = (options: {
  readonly client: GitlabClient.Interface;
  readonly cable: Cable.Interface;
  readonly identity?: Effect.Effect<TokenIdentity, SourceError> | undefined;
}): Effect.Effect<Source> =>
  Gitlab.source.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(
          GitlabIdentity.Service,
          GitlabIdentity.Service.of({
            host: "gitlab.test",
            identity: options.identity ?? Effect.succeed(identity),
          }),
        ),
        Layer.succeed(GitlabClient.Service, GitlabClient.Service.of(options.client)),
        Layer.succeed(Cable.Service, Cable.Service.of(options.cable)),
      ),
    ),
  );

const runSource = <A, E>(
  options: {
    readonly client: GitlabClient.Interface;
    readonly cable: Cable.Interface;
    readonly identity?: Effect.Effect<TokenIdentity, SourceError> | undefined;
  },
  body: (source: Source) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* sourceWith(options);
        return yield* body(source);
      }),
    ),
  );

const eventNamed = (source: Source, name: string) => {
  const event = source.events.find((candidate) => candidate.name === name);
  if (event === undefined) throw new Error(`missing event ${name}`);
  return event;
};

const onlyQueue = (fake: FakeCable) => {
  const queue = fake.queues[0];
  if (queue === undefined) throw new Error("missing cable subscription");
  return queue;
};

test("drops the initial null and maps the next payload to an occurrence", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const values = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "ciPipelineStatusesUpdated");
      const subscription = yield* event.subscribe({ projectFullPath: "glints/example" });
      const fiber = yield* subscription.occurrences.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      const queue = onlyQueue(fake);
      yield* Queue.offer(queue, {
        result: { data: { ciPipelineStatusesUpdated: null } },
        more: true,
      });
      yield* Queue.offer(queue, {
        result: {
          data: {
            ciPipelineStatusesUpdated: {
              id: "gid://gitlab/Ci::Pipeline/463669",
              status: "FAILED",
              updatedAt: "2026-11-21T08:00:00Z",
            },
          },
        },
        more: true,
      });
      return yield* Fiber.join(fiber);
    }),
  );
  expect(values).toHaveLength(1);
  const occurrence = values[0];
  if (occurrence === undefined) throw new Error("missing occurrence");
  expect(occurrence.id).toBeUndefined();
  expect(occurrence.payload).toEqual({
    id: "gid://gitlab/Ci::Pipeline/463669",
    status: "FAILED",
    updatedAt: "2026-11-21T08:00:00Z",
  });
  expect(client.gets).toEqual([{ path: "/projects/glints%2Fexample" }]);
});

test("rejects an inaccessible project before any subscribe frame", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient({
    getError: new Rejected({ message: "GitLab HTTP 404 on /projects: not found" }),
  });
  const error = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "ciPipelineStatusesUpdated");
      return yield* Effect.scoped(event.subscribe({ projectFullPath: "glints/private" })).pipe(
        Effect.flip,
      );
    }),
  );
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("Project glints/private is not accessible");
  expect(fake.identifiers).toHaveLength(0);
});

test("rejects a database id where a global ID is required", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const source = await Effect.runPromise(sourceWith({ client: client.client, cable: fake.cable }));
  const h = await connect({ sources: [source] });
  try {
    const failure = Schema.decodeUnknownSync(
      Schema.Struct({ code: Schema.Number, message: Schema.String }),
    )(
      await h.stream(
        {
          watchId: "database-id",
          params: { pipelineId: "463669" },
          prepare: "return null",
          transform: "return null",
          expiresInSeconds: 1,
        },
        "gitlab.ciPipelineStatusUpdated",
      ).done,
    );
    expect(failure.code).toBe(-32602);
    expect(failure.message).toContain("gid://gitlab/Ci::Pipeline/");
    expect(failure.message).toContain("not the number in the web URL");
    expect(fake.identifiers).toHaveLength(0);
  } finally {
    await h.close();
  }
});

test("rejects a user that is not the token's user before any subscribe frame", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const error = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "userMergeRequestUpdated");
      return yield* Effect.scoped(event.subscribe({ userId: "gid://gitlab/User/1" })).pipe(
        Effect.flip,
      );
    }),
  );
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("gid://gitlab/User/256");
  expect(fake.identifiers).toHaveLength(0);
});

test("rejects a watch with no token without touching GitLab", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const error = await runSource(
    {
      client: client.client,
      cable: fake.cable,
      identity: Effect.fail(
        new Rejected({
          message: "No GitLab token for gitlab.test. Set GITLAB_TOKEN, or authenticate glab.",
        }),
      ),
    },
    (source) =>
      Effect.gen(function* () {
        const event = eventNamed(source, "ciPipelineStatusesUpdated");
        return yield* Effect.scoped(event.subscribe({ projectFullPath: "glints/example" })).pipe(
          Effect.flip,
        );
      }),
  );
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("GITLAB_TOKEN");
  expect(client.gets).toHaveLength(0);
  expect(fake.identifiers).toHaveLength(0);
});

test("subscribes to a merge request's notes and maps an updated signal", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const values = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "noteable.notes_changed");
      const subscription = yield* event.subscribe({
        projectId: 599,
        noteableType: "merge_request",
        noteableId: 48090,
      });
      const fiber = yield* subscription.occurrences.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Queue.offer(onlyQueue(fake), { event: "updated" });
      return yield* Fiber.join(fiber);
    }),
  );
  expect(fake.identifiers).toEqual([
    {
      channel: "Noteable::NotesChannel",
      project_id: 599,
      group_id: null,
      noteable_type: "merge_request",
      noteable_id: 48090,
    },
  ]);
  expect(client.graphqls[0]?.variables).toEqual({ id: "gid://gitlab/MergeRequest/48090" });
  const occurrence = values[0];
  if (occurrence === undefined) throw new Error("missing occurrence");
  expect(occurrence.id).toBeUndefined();
  expect(occurrence.occurredAt).toBeUndefined();
  expect(occurrence.payload).toEqual({
    event: "updated",
    projectId: 599,
    noteableType: "merge_request",
    noteableId: 48090,
    noteableIid: 32,
  });
});

test("subscribes to an issue by its own database id", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "noteable.notes_changed");
      yield* Effect.scoped(
        event.subscribe({ projectId: 18, noteableType: "issue", noteableId: 1183 }),
      );
    }),
  );
  expect(fake.identifiers).toEqual([
    {
      channel: "Noteable::NotesChannel",
      project_id: 18,
      group_id: null,
      noteable_type: "issue",
      noteable_id: 1183,
    },
  ]);
  expect(client.graphqls[0]?.variables).toEqual({ id: "gid://gitlab/Issue/1183" });
  expect(client.graphqls[0]?.query).toContain("issue(id: $id)");
});

test("rejects an inaccessible noteable before any subscribe frame", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient({ graphql: () => ({ mergeRequest: null }) });
  const error = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "noteable.notes_changed");
      return yield* Effect.scoped(
        event.subscribe({ projectId: 599, noteableType: "merge_request", noteableId: 48090 }),
      ).pipe(Effect.flip);
    }),
  );
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("not found or is not accessible");
  expect(fake.identifiers).toHaveLength(0);
});

test("delivers a merge request event whose updatedAt is old", async () => {
  const fake = makeFakeCable();
  const client = makeFakeClient();
  const values = await runSource({ client: client.client, cable: fake.cable }, (source) =>
    Effect.gen(function* () {
      const event = eventNamed(source, "mergeRequestApprovalStateUpdated");
      const subscription = yield* event.subscribe({ issuableId: "gid://gitlab/MergeRequest/1" });
      const fiber = yield* subscription.occurrences.pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* Queue.offer(onlyQueue(fake), {
        result: {
          data: {
            mergeRequestApprovalStateUpdated: {
              id: "gid://gitlab/MergeRequest/1",
              iid: "32",
              updatedAt: "2026-11-21T05:00:00Z",
              approved: true,
            },
          },
        },
        more: true,
      });
      return yield* Fiber.join(fiber);
    }),
  );
  const occurrence = values[0];
  if (occurrence === undefined) throw new Error("missing occurrence");
  expect(occurrence.payload).toMatchObject({
    id: "gid://gitlab/MergeRequest/1",
    updatedAt: "2026-11-21T05:00:00Z",
    approved: true,
  });
});
