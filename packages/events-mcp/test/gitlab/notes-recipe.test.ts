import { expect, test, vi } from "vitest";
import { Effect, Layer, Queue, Redacted, Schema, Stream } from "effect";
import type { GetInput } from "../../src/core/tool.ts";
import { GitlabSocket } from "../../src/gitlab/socket.ts";
import { GitlabClient } from "../../src/gitlab/client.ts";
import { GitlabAuth } from "../../src/gitlab/auth.ts";
import { notesRecipe } from "../../src/gitlab/notes.ts";
import { Gitlab } from "../../src/gitlab/integration.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { Health, type IntegrationError } from "../../src/core/integration.ts";
import { connect } from "../harness.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const NotesOutput = Schema.Struct({
  notes: Schema.Array(Schema.Struct({ id: Schema.Number })),
});

test("the notes recipe delivers the first new comment after START and nothing else", async () => {
  const recipe = notesRecipe.replace("const START = 0;", "const START = 12;");
  const responses: Array<Schema.Json> = [
    [
      { id: 14, system: false, body: "the first new comment" },
      { id: 13, system: true, body: "added a label" },
      { id: 12, system: false, body: "before arming" },
    ],
    [
      { id: 14, system: false, body: "the first new comment" },
      { id: 13, system: true, body: "added a label" },
      { id: 12, system: false, body: "before arming" },
    ],
    [{ id: 16, system: true, body: "assigned" }],
    [
      { id: 17, system: false, body: "a later comment" },
      { id: 14, system: false, body: "the first new comment" },
    ],
  ];
  const responseCount = responses.length;
  const queuePromise = Promise.withResolvers<Queue.Queue<Schema.Json, IntegrationError>>();
  const gets: Array<GetInput> = [];
  const client: GitlabClient.Interface = {
    get: (input) => {
      gets.push(input);
      return Effect.succeed(responses.shift() ?? []);
    },
    graphql: () =>
      Effect.succeed({
        mergeRequest: {
          id: "gid://gitlab/MergeRequest/48090",
          iid: "32",
          projectId: 599,
        },
      }),
  };
  const socket: GitlabSocket.Interface = {
    subscribe: () =>
      Effect.gen(function* () {
        const queue = yield* Queue.unbounded<Schema.Json, IntegrationError>();
        queuePromise.resolve(queue);
        return Stream.fromQueue(queue);
      }),
    health: Stream.succeed(Health.Live()),
  };
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
    h.stream(
      {
        watchId: "comments",
        params: { projectId: 599, noteableType: "merge_request", noteableId: 48090 },
        script: recipe,
        expiresInSeconds: 60,
      },
      "gitlab.noteable.notes_changed",
    );
    await vi.waitFor(() =>
      expect(h.notifications.filter((notification) => notification.type === "active")).toHaveLength(
        1,
      ),
    );
    const queue = await queuePromise.promise;
    for (let index = 0; index < responseCount; index += 1) {
      await Effect.runPromise(Queue.offer(queue, { event: "updated" }));
    }
    await vi.waitFor(() =>
      expect(
        h.notifications.some(
          (notification) =>
            notification.type === "event" && String(notification.data).includes('"id":17'),
        ),
      ).toBe(true),
    );
    const delivered = h.notifications
      .filter((notification) => notification.type === "event")
      .map((notification) =>
        Schema.decodeUnknownSync(NotesOutput)(JSON.parse(String(notification.data))).notes.map(
          (note) => note.id,
        ),
      );
    expect(delivered).toEqual([[14], [17]]);
    expect(gets).toHaveLength(responseCount);
    expect(gets[0]).toEqual({
      path: "/projects/599/merge_requests/32/notes",
      query: { sort: "desc", order_by: "created_at", per_page: 20 },
    });
  } finally {
    await h.close();
  }
});
