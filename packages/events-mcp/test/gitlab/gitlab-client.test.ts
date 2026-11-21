import { expect, test } from "vitest";
import { Effect, Redacted, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeGetTool, makeGitlabClient } from "../../src/gitlab/client.ts";
import type { GitlabIdentity } from "../../src/gitlab/token.ts";
import { SourceName } from "../../src/core/source.ts";
import { connect } from "../harness.ts";

const identity: GitlabIdentity = {
  token: Redacted.make("test-token"),
  userId: 256,
  username: "tester",
};

const makeHttp = (respond: () => Response) => {
  const urls: string[] = [];
  const http = HttpClient.make((request) => {
    urls.push(request.url);
    return Effect.succeed(HttpClientResponse.fromWeb(request, respond()));
  });
  return { http, urls };
};

const clientWith = (http: HttpClient.HttpClient) =>
  Effect.runPromise(
    makeGitlabClient({ host: "gitlab.test", identity: Effect.succeed(identity), http }),
  );

test("the REST client keeps a percent-encoded project path under /api/v4", async () => {
  const fake = makeHttp(() => Response.json({ id: 599 }));
  const client = await clientWith(fake.http);
  await Effect.runPromise(client.get({ path: "/projects/glints%2Fotp", query: { simple: true } }));
  expect(fake.urls).toEqual(["https://gitlab.test/api/v4/projects/glints%2Fotp?simple=true"]);
});

test("the REST client rejects a percent-encoded path traversal", async () => {
  const fake = makeHttp(() => Response.json({ id: 1 }));
  const client = await clientWith(fake.http);
  const error = await Effect.runPromise(
    client.get({ path: "/projects/%2e%2e/%2e%2e/admin" }).pipe(Effect.flip),
  );
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("/api/v4");
  expect(fake.urls).toEqual([]);
});

test("the REST client treats a redirect as an error rather than following it", async () => {
  const fake = makeHttp(
    () => new Response(null, { status: 302, headers: { location: "https://evil.test/" } }),
  );
  const client = await clientWith(fake.http);
  const error = await Effect.runPromise(client.get({ path: "/user" }).pipe(Effect.flip));
  expect(error._tag).toBe("Source.Rejected");
  expect(error.message).toContain("HTTP 302");
  expect(fake.urls).toEqual(["https://gitlab.test/api/v4/user"]);
});

test("gitlab_get reports an HTTP body that is not valid JSON", async () => {
  const fake = makeHttp(
    () =>
      new Response('{"value":1e400}', {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
  );
  const client = await clientWith(fake.http);
  const h = await connect({
    sources: [
      {
        name: Schema.decodeUnknownSync(SourceName)("gitlab"),
        tools: { get: makeGetTool(client) },
        events: [],
      },
    ],
  });
  try {
    const result = await h.callRaw("gitlab_get", { path: "/test" });
    expect(result.isError).toBe(true);
    expect(result.text).toContain("GitLab returned invalid JSON from /test");
  } finally {
    await h.close();
  }
});
