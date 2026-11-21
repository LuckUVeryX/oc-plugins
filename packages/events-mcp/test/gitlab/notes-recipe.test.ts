import { expect, test, vi } from "vitest";
import { Deferred, Effect, Fiber, Queue, Redacted, Schema, Stream } from "effect";
import type { Cable } from "../../src/gitlab/cable.ts";
import type { GetInput } from "../../src/core/tool.ts";
import type { GitlabClient } from "../../src/gitlab/client.ts";
import { notesRecipe } from "../../src/gitlab/notes.ts";
import { makeGitlabSource } from "../../src/gitlab/source.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { makeCatalog } from "../../src/core/catalog.ts";
import { makePrepare, makeWatchState } from "../../src/core/prepare.ts";
import { makeTransform } from "../../src/core/transform.ts";
import { makeUpstream } from "../../src/core/upstream.ts";
import { Health, SourceError } from "../../src/core/source.ts";
import { runWatch } from "../../src/core/watch.ts";

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
  const queuePromise = Promise.withResolvers<Queue.Queue<Schema.Json, SourceError>>();
  const notifications: Array<{ type: string; data?: unknown }> = [];
  const gets: Array<GetInput> = [];
  const client: GitlabClient = {
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
  const cable: Cable = {
    subscribeUnique: () =>
      Effect.gen(function* () {
        const queue = yield* Queue.unbounded<Schema.Json, SourceError>();
        queuePromise.resolve(queue);
        return Stream.fromQueue(queue);
      }),
    health: Stream.succeed(Health.Live()),
  };
  const fiber = Effect.runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const source = yield* makeGitlabSource({
          host: "gitlab.test",
          identity: Effect.succeed(identity),
          client,
          cable,
        });
        const catalog = yield* makeCatalog([source]);
        const event = catalog.get("gitlab.noteable.notes_changed");
        if (event === undefined) return yield* Effect.die(new Error("missing notes event"));
        const params = { projectId: 599, noteableType: "merge_request", noteableId: 48090 };
        const state = yield* makeWatchState;
        const prepare = makePrepare({ sources: [source], state });
        const transform = makeTransform();
        yield* runWatch({
          watchId: "comments",
          eventType: event,
          params,
          filter: [],
          expiresInSeconds: 60,
          eventTimeoutSeconds: 10,
          ended: yield* Deferred.make<string>(),
          upstream: yield* makeUpstream(catalog),
          prepare: (occurrence) => prepare(recipe, occurrence),
          transform: (input) => transform("return JSON.stringify(input.prepared)", input),
          notify: (type, body) =>
            Effect.sync(() => {
              notifications.push({ type, ...body });
            }),
        });
      }),
    ),
  );
  try {
    const queue = await queuePromise.promise;
    for (let index = 0; index < responseCount; index += 1) {
      await Effect.runPromise(Queue.offer(queue, { event: "updated" }));
    }
    await vi.waitFor(() =>
      expect(
        notifications.some(
          (notification) =>
            notification.type === "event" && String(notification.data).includes('"id":17'),
        ),
      ).toBe(true),
    );
    const delivered = notifications
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
    await Effect.runPromise(Fiber.interrupt(fiber));
  }
});
