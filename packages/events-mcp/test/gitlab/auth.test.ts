import { ConfigProvider, Context, Effect, Layer } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { expect, test } from "vitest";
import { GitlabAuth } from "../../src/gitlab/auth.ts";

test("the auth layer retries a failed token check and caches the success", async () => {
  let attempts = 0;
  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      attempts += 1;
      return attempts === 1
        ? HttpClientResponse.fromWeb(request, new Response(null, { status: 503 }))
        : HttpClientResponse.fromWeb(request, Response.json({ id: 256, username: "tester" }));
    }),
  );
  const config = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      GITLAB_HOST: "gitlab.test",
      GITLAB_TOKEN: "test-token",
    }),
  );
  const context = await Effect.runPromise(
    Effect.scoped(
      Layer.build(
        GitlabAuth.layer.pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
          Layer.provide(config),
        ),
      ),
    ),
  );
  const identity = Context.get(context, GitlabAuth.Service);
  const failure = await Effect.runPromise(
    Effect.flip(identity.identity).pipe(Effect.provide(config)),
  );
  expect(failure._tag).toBe("Integration.Unavailable");
  const first = await Effect.runPromise(identity.identity.pipe(Effect.provide(config)));
  expect(first.username).toBe("tester");
  const second = await Effect.runPromise(identity.identity.pipe(Effect.provide(config)));
  expect(second.username).toBe("tester");
  expect(attempts).toBe(2);
});
