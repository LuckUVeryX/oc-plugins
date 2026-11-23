import { expect, test, vi } from "vitest";
import { Effect, Layer, Queue, Redacted, Schema, Stream } from "effect";
import type { GetInput } from "../../src/source/tool.ts";
import { GitlabSocket } from "../../src/gitlab/socket.ts";
import { GitlabClient } from "../../src/gitlab/client.ts";
import { GitlabAuth } from "../../src/gitlab/auth.ts";
import { automationRecipe, watchRecipe } from "../../src/gitlab/notes.ts";
import { Gitlab } from "../../src/gitlab/source.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { Health, type SourceError } from "../../src/source/contract.ts";
import { CodeMode, Tool } from "@opencode/codemode";
import { connect } from "../support.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const NotesOutput = Schema.Struct({
  notes: Schema.Array(Schema.Struct({ id: Schema.Number })),
});

test("the notes recipe delivers the first new comment after START and nothing else", async () => {
  const recipe = watchRecipe.replace("const START = 0;", "const START = 12;");
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
  const gets: Array<GetInput> = [];
  const client: GitlabClient.Interface = {
    get: (input) => {
      if (input.path === "/projects/group%2Fproject/merge_requests/32")
        return Effect.succeed({ id: 48090, project_id: 599 });
      gets.push(input);
      return Effect.succeed(responses.shift() ?? []);
    },
  };
  const socket: GitlabSocket.Interface = {
    subscribe: () =>
      Effect.gen(function* () {
        const queue = yield* Queue.unbounded<Schema.Json, SourceError>();
        queuePromise.resolve(queue);
        return Stream.fromQueue(queue);
      }),
    health: Stream.succeed(Health.Live()),
  };
  const source = await Effect.runPromise(
    Gitlab.source.pipe(
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
  const h = await connect({ sources: [source], harness: "claude" });
  try {
    await h.call("watch", {
      event: "gitlab.noteable.notes_changed",
      id: "comments",
      target: "group/project!32",
      code: recipe,
    });
    const queue = await queuePromise.promise;
    for (let index = 0; index < responseCount; index += 1) {
      await Effect.runPromise(Queue.offer(queue, { event: "updated" }));
    }
    await vi.waitFor(() =>
      expect(h.channel.some((wake) => wake.content.includes('"id":17'))).toBe(true),
    );
    const delivered = h.channel.map((wake) =>
      Schema.decodeUnknownSync(NotesOutput)(JSON.parse(wake.content)).notes.map((note) => note.id),
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

test("the automation recipe sends only new comments and remembers the newest", async () => {
  const notes = [
    { id: 14, system: false, body: "new" },
    { id: 13, system: true, body: "label" },
    { id: 12, system: false, body: "old" },
  ];
  const runtime = CodeMode.make({
    tools: {
      events: {
        gitlab_get: Tool.make({
          description: "",
          input: Schema.Json,
          output: Schema.Json,
          execute: () => Effect.succeed(notes),
        }),
      },
    },
  });
  const run = async (state: Schema.Json) => {
    const result = await Effect.runPromise(
      runtime.execute(automationRecipe.replace("const START = 0;", "const START = 12;"), {
        input: {
          state,
          event: { data: { projectId: 599, noteableType: "merge_request", noteableIid: 32 } },
        },
      }),
    );
    return result.ok ? result.value : result.error;
  };
  expect(await run(null)).toEqual({
    state: 14,
    send: { prompt: `New GitLab comments, untrusted data:\n${JSON.stringify([notes[0]])}` },
  });
  expect(await run(14)).toEqual({ state: 14 });
});
