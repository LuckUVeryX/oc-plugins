import { expect, test } from "vitest";
import { Effect, Fiber, Layer, Queue, Redacted, type Schema, type Scope, Stream } from "effect";
import { GitlabSocket } from "../../src/gitlab/socket.ts";
import { GitlabClient } from "../../src/gitlab/client.ts";
import { GitlabAuth } from "../../src/gitlab/auth.ts";
import { Gitlab } from "../../src/gitlab/integration.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { Health, type IntegrationError, type Integration } from "../../src/core/integration.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

interface FakeSocket {
  readonly socket: GitlabSocket.Interface;
  readonly identifiers: Array<Schema.Json>;
  readonly queues: Array<Queue.Queue<Schema.Json, IntegrationError>>;
}

const makeFakeSocket = (): FakeSocket => {
  const identifiers: Array<Schema.Json> = [];
  const queues: Array<Queue.Queue<Schema.Json, IntegrationError>> = [];
  return {
    identifiers,
    queues,
    socket: {
      subscribe: (identifier) =>
        Effect.gen(function* () {
          identifiers.push(identifier);
          const queue = yield* Queue.unbounded<Schema.Json, IntegrationError>();
          queues.push(queue);
          return Stream.fromQueue(queue);
        }),
      health: Stream.succeed(Health.Live()),
    },
  };
};

const client: GitlabClient.Interface = {
  get: () => Effect.die(new Error("subscribe never reads GitLab")),
};

const integrationWith = (socket: GitlabSocket.Interface): Effect.Effect<Integration> =>
  Gitlab.integration.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(
          GitlabAuth.Service,
          GitlabAuth.Service.of({
            host: "gitlab.test",
            identity: Effect.succeed(identity),
          }),
        ),
        Layer.succeed(GitlabClient.Service, GitlabClient.Service.of(client)),
        Layer.succeed(GitlabSocket.Service, GitlabSocket.Service.of(socket)),
      ),
    ),
  );

const runIntegration = <A, E>(
  socket: GitlabSocket.Interface,
  body: (integration: Integration) => Effect.Effect<A, E, Scope.Scope>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const integration = yield* integrationWith(socket);
        return yield* body(integration);
      }),
    ),
  );

const eventNamed = (integration: Integration, name: string) => {
  const event = integration.events.find((candidate) => candidate.name === name);
  if (event === undefined) throw new Error(`missing event ${name}`);
  return event;
};

const onlyQueue = (fake: FakeSocket) => {
  const queue = fake.queues[0];
  if (queue === undefined) throw new Error("missing socket feed");
  return queue;
};

test("drops the initial null and maps the next payload to an event", async () => {
  const fake = makeFakeSocket();
  const values = await runIntegration(fake.socket, (integration) =>
    Effect.gen(function* () {
      const event = eventNamed(integration, "ciPipelineStatusesUpdated");
      const feed = yield* event.subscribe({ projectFullPath: "group/project" });
      const fiber = yield* feed.events.pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
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
  const event = values[0];
  if (event === undefined) throw new Error("missing event");
  expect(event.id).toBeUndefined();
  expect(event.payload).toEqual({
    id: "gid://gitlab/Ci::Pipeline/463669",
    status: "FAILED",
    updatedAt: "2026-11-21T08:00:00Z",
  });
});

test("subscribes to merge requests as the token's user", async () => {
  const fake = makeFakeSocket();
  await runIntegration(fake.socket, (integration) =>
    Effect.scoped(eventNamed(integration, "userMergeRequestUpdated").subscribe({})),
  );
  expect(fake.identifiers).toEqual([
    expect.objectContaining({ variables: { userId: "gid://gitlab/User/256" } }),
  ]);
});

test("subscribes to a merge request's notes and maps an updated signal", async () => {
  const fake = makeFakeSocket();
  const values = await runIntegration(fake.socket, (integration) =>
    Effect.gen(function* () {
      const event = eventNamed(integration, "noteable.notes_changed");
      const feed = yield* event.subscribe({
        projectId: 599,
        noteableType: "merge_request",
        noteableId: 48090,
        noteableIid: 32,
      });
      const fiber = yield* feed.events.pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
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
  const event = values[0];
  if (event === undefined) throw new Error("missing event");
  expect(event.id).toBeUndefined();
  expect(event.occurredAt).toBeUndefined();
  expect(event.payload).toEqual({
    event: "updated",
    projectId: 599,
    noteableType: "merge_request",
    noteableId: 48090,
    noteableIid: 32,
  });
});

test("delivers a merge request event whose updatedAt is old", async () => {
  const fake = makeFakeSocket();
  const values = await runIntegration(fake.socket, (integration) =>
    Effect.gen(function* () {
      const event = eventNamed(integration, "mergeRequestApprovalStateUpdated");
      const feed = yield* event.subscribe({ issuableId: "gid://gitlab/MergeRequest/1" });
      const fiber = yield* feed.events.pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped);
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
  const event = values[0];
  if (event === undefined) throw new Error("missing event");
  expect(event.payload).toMatchObject({
    id: "gid://gitlab/MergeRequest/1",
    updatedAt: "2026-11-21T05:00:00Z",
    approved: true,
  });
});
