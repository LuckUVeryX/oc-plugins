import { expect, test } from "vitest";
import { Effect } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeClient, makeGetTool } from "../../src/codemagic/client.ts";

test("codemagic_get cannot leave /api/v3 with the token", async () => {
  const urls: Array<string> = [];
  const http = HttpClient.make((request, url) => {
    urls.push(url.toString());
    return Effect.succeed(HttpClientResponse.fromWeb(request, Response.json({ data: [] })));
  });
  const tool = makeGetTool(makeClient({ token: Effect.succeed("test"), http }));
  for (const path of ["/../x", "//evil.test", "/\\evil.test", "/%2e%2e/x", "/.\t./x", "/x?y=1"])
    await Effect.runPromise(tool.run({ path }).pipe(Effect.flip));
  expect(urls).toEqual([]);
  await Effect.runPromise(tool.run({ path: "/builds/1/actions", query: { page: 2 } }));
  expect(urls).toEqual(["https://codemagic.io/api/v3/builds/1/actions?page=2"]);
});
