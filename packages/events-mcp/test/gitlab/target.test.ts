import { expect, test, vi } from "vitest";
import { Effect, Layer, Queue, Redacted, Schema, Stream } from "effect";
import { GitlabSocket } from "../../src/gitlab/socket.ts";
import type { GetInput } from "../../src/core/tool.ts";
import { GitlabClient, type GraphqlRequest } from "../../src/gitlab/client.ts";
import { GitlabAuth } from "../../src/gitlab/auth.ts";
import { Gitlab } from "../../src/gitlab/integration.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import {
  Health,
  Rejected,
  Unavailable,
  type IntegrationError,
  type Integration,
} from "../../src/core/integration.ts";
import { connect } from "../harness.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const sha = "0123456789abcdef0123456789abcdef01234567";
const mergeRequest = { projectId: 599, noteableType: "merge_request", noteableId: 48090 };
const issue = { projectId: 599, noteableType: "issue", noteableId: 1183 };

const resources: Readonly<Record<string, Schema.Json>> = {
  "/projects/group%2Fproject": { id: 599 },
  "/projects/group%2Fproject/merge_requests/32": { id: 48090, project_id: 599 },
  "/projects/group%2Fproject/issues/30": { id: 1183, project_id: 599 },
  "/projects/group%2Fproject/pipelines/463669": { id: 463669 },
  "/projects/group%2Fproject/jobs/77": { id: 77 },
  [`/projects/group%2Fproject/repository/commits/${sha}`]: { id: sha },
  "/projects/a%2Fb%2Fc%2Fproject/merge_requests/32": { id: 48090, project_id: 599 },
};

const makeFakeSocket = () => {
  const identifiers: Array<Schema.Json> = [];
  const socket: GitlabSocket.Interface = {
    subscribe: (identifier) =>
      Effect.gen(function* () {
        identifiers.push(identifier);
        return Stream.fromQueue(yield* Queue.unbounded<Schema.Json, IntegrationError>());
      }),
    health: Stream.succeed(Health.Live()),
  };
  return { identifiers, socket };
};

const makeFakeClient = (getError?: IntegrationError) => {
  const gets: Array<GetInput> = [];
  const graphqls: Array<GraphqlRequest> = [];
  const client: GitlabClient.Interface = {
    get: (input) => {
      gets.push(input);
      if (getError !== undefined) return Effect.fail(getError);
      const body = resources[input.path];
      return body === undefined
        ? Effect.fail(new Rejected({ message: `GitLab HTTP 404 on ${input.path}: Not found` }))
        : Effect.succeed(body);
    },
    graphql: (request) => {
      graphqls.push(request);
      return Effect.succeed({
        mergeRequest: {
          id: "gid://gitlab/MergeRequest/48090",
          iid: "32",
          projectId: 599,
        },
        issue: {
          id: "gid://gitlab/Issue/1183",
          iid: "30",
          projectId: 599,
        },
      });
    },
  };
  return { client, gets, graphqls };
};

const setup = async (getError?: IntegrationError) => {
  const socket = makeFakeSocket();
  const client = makeFakeClient(getError);
  const integration: Integration = await Effect.runPromise(
    Gitlab.integration.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(
            GitlabAuth.Service,
            GitlabAuth.Service.of({ host: "gitlab.test", identity: Effect.succeed(identity) }),
          ),
          Layer.succeed(GitlabClient.Service, GitlabClient.Service.of(client.client)),
          Layer.succeed(GitlabSocket.Service, GitlabSocket.Service.of(socket.socket)),
        ),
      ),
    ),
  );
  return { integration, socket, client };
};

const scripts = { prepare: "return null", transform: "return null" };

const subscriptionOf = async (event: string, args: Record<string, unknown>) => {
  const { integration, socket } = await setup();
  const h = await connect({ integrations: [integration] });
  try {
    h.stream({ ...scripts, ...args }, event);
    await vi.waitFor(() => expect(socket.identifiers).toHaveLength(1));
    const identifier = socket.identifiers[0];
    if (identifier === undefined) throw new Error("missing subscription identifier");
    return identifier;
  } finally {
    await h.close();
  }
};

const withoutNonce = (identifier: Schema.Json) => {
  if (typeof identifier !== "object" || identifier === null || Array.isArray(identifier))
    throw new Error("expected an object subscription identifier");
  const { nonce: _, ...rest } = identifier as Record<string, Schema.Json>;
  return rest;
};

const rejectionOf = async (
  event: string,
  args: Record<string, unknown>,
  getError?: IntegrationError,
) => {
  const { integration, socket } = await setup(getError);
  const h = await connect({ integrations: [integration] });
  try {
    const failure = Schema.decodeUnknownSync(
      Schema.Struct({ code: Schema.Number, message: Schema.String }),
    )(await h.stream({ ...scripts, ...args }, event).done);
    return { failure, identifiers: socket.identifiers };
  } finally {
    await h.close();
  }
};

const equivalent = [
  [
    "gitlab.noteable.notes_changed",
    "https://gitlab.test/group/project/-/merge_requests/32",
    mergeRequest,
  ],
  ["gitlab.noteable.notes_changed", "/group/project/-/merge_requests/32", mergeRequest],
  [
    "gitlab.noteable.notes_changed",
    "https://gitlab.test/group%2Fproject/-/merge_requests/32",
    mergeRequest,
  ],
  ["gitlab.noteable.notes_changed", "https://gitlab.test/group/project/-/issues/30", issue],
  ["gitlab.noteable.notes_changed", "group/project#30", issue],
  ["gitlab.noteable.notes_changed", "https://gitlab.test/group/project/-/work_items/30", issue],
  [
    "gitlab.noteable.notes_changed",
    "https://gitlab.test/a/b/c/project/-/merge_requests/32",
    mergeRequest,
  ],
  [
    "gitlab.mergeRequestApprovalStateUpdated",
    "group/project!32",
    { issuableId: "gid://gitlab/MergeRequest/48090" },
  ],
  [
    "gitlab.ciPipelineStatusUpdated",
    "https://gitlab.test/group/project/-/pipelines/463669",
    { pipelineId: "gid://gitlab/Ci::Pipeline/463669" },
  ],
  [
    "gitlab.ciPipelineStatusUpdated",
    "group/project/-/pipelines/463669",
    { pipelineId: "gid://gitlab/Ci::Pipeline/463669" },
  ],
  [
    "gitlab.ciJobStatusUpdated",
    "https://gitlab.test/group/project/-/jobs/77",
    { jobId: "gid://gitlab/Ci::Build/77" },
  ],
  ["gitlab.ciPipelineStatusesUpdated", "group/project", { projectFullPath: "group/project" }],
  [
    "gitlab.ciPipelineStatusesUpdated",
    `https://gitlab.test/group/project/-/commit/${sha}`,
    { projectFullPath: "group/project", sha },
  ],
  [
    "gitlab.ciPipelineStatusesUpdated",
    `https://gitlab.test/group/project/-/commit/${sha.toUpperCase()}`,
    { projectFullPath: "group/project", sha },
  ],
] as const;

test("a target arms the same subscription as the params it resolves to", async () => {
  for (const [event, target, params] of equivalent) {
    const fromTarget = await subscriptionOf(event, { watchId: "from-target", target });
    const fromParams = await subscriptionOf(event, { watchId: "from-params", params });
    expect(withoutNonce(fromTarget)).toEqual(withoutNonce(fromParams));
  }
});

test("a target that cannot arm the watch is rejected before subscribing", async () => {
  const cases = [
    {
      event: "gitlab.noteable.notes_changed",
      args: {
        watchId: "foreign-host",
        target: "https://gitlab.com/group/project/-/merge_requests/32",
      },
      message: ["gitlab.com", "gitlab.test"],
    },
    {
      event: "gitlab.ciJobStatusUpdated",
      args: {
        watchId: "wrong-kind",
        target: "https://gitlab.test/group/project/-/merge_requests/32",
      },
      message: ["gitlab.ciJobStatusUpdated takes a job link", "not a merge request link"],
    },
    {
      event: "gitlab.ciPipelineStatusesUpdated",
      args: {
        watchId: "missing-commit",
        target: `https://gitlab.test/group/project/-/commit/${"f".repeat(40)}`,
      },
      message: ["404", "repository/commits"],
    },
    {
      event: "gitlab.noteable.notes_changed",
      args: { watchId: "file-scheme", target: "file:///group/project/-/merge_requests/32" },
      message: ["must be an http or https link"],
    },
    {
      event: "gitlab.noteable.notes_changed",
      args: {
        watchId: "protocol-relative",
        target: "//gitlab.test/group/project/-/merge_requests/32",
      },
      message: ["must be an http or https link"],
    },
    {
      event: "gitlab.noteable.notes_changed",
      args: { watchId: "space", target: "group/project #30" },
      message: ["not valid for gitlab.noteable.notes_changed"],
    },
    {
      event: "gitlab.noteable.notes_changed",
      args: { watchId: "empty-suffix", target: "group/project#" },
      message: ["needs an iid or sha"],
    },
    {
      event: "gitlab.mergeRequestApprovalStateUpdated",
      args: {
        watchId: "both",
        params: { issuableId: "gid://gitlab/MergeRequest/48090" },
        target: "group/project!32",
      },
      message: ["Pass params or target, not both"],
    },
    {
      event: "gitlab.userMergeRequestUpdated",
      args: { watchId: "no-target", target: "group/project" },
      message: ["gitlab.userMergeRequestUpdated does not take a target"],
    },
  ] as const;
  for (const item of cases) {
    const { failure, identifiers } = await rejectionOf(item.event, item.args);
    expect(failure.code).toBe(-32602);
    for (const text of item.message) expect(failure.message).toContain(text);
    expect(identifiers).toHaveLength(0);
  }
});

test("a target that GitLab cannot answer for is an internal error", async () => {
  const { failure, identifiers } = await rejectionOf(
    "gitlab.noteable.notes_changed",
    { watchId: "unavailable", target: "https://gitlab.test/group/project/-/merge_requests/32" },
    new Unavailable({ message: "GitLab is restarting" }),
  );
  expect(failure.code).toBe(-32603);
  expect(failure.message).toContain("GitLab is restarting");
  expect(identifiers).toHaveLength(0);
});

test("the watch tool returns the params a target resolved to", async () => {
  const { integration, socket } = await setup();
  const h = await connect({ integrations: [integration], harness: "claude" });
  try {
    const armed = await h.call("watch", {
      event: "gitlab.noteable.notes_changed",
      watchId: "mr-comments",
      target: "group/project!32",
      ...scripts,
    });
    expect(armed).toMatchObject({ watchId: "mr-comments", params: mergeRequest });
    expect(armed.expiresAt).toEqual(expect.any(String));
    expect(socket.identifiers).toHaveLength(1);
  } finally {
    await h.close();
  }
});

test("list_events offers target only where the event can resolve one", async () => {
  const { integration } = await setup();
  const h = await connect({ integrations: [integration], harness: "claude" });
  try {
    const listed = await h.call("list_events");
    type InputSchema = {
      properties?: Record<string, { description?: string }>;
    };
    const byName = new Map<string, InputSchema>(
      listed.events.map((event: { name: string; inputSchema: InputSchema }) => [
        event.name,
        event.inputSchema,
      ]),
    );
    expect(byName.get("gitlab.ciJobStatusUpdated")?.properties?.target?.description).toContain(
      "Instead of params, pass a job link",
    );
    expect(byName.get("gitlab.userMergeRequestUpdated")?.properties?.target).toBeUndefined();
  } finally {
    await h.close();
  }
});
