import { expect, test } from "vitest";
import { Effect } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeClient } from "../../src/codemagic/client.ts";
import { makeStartTool } from "../../src/codemagic/tools.ts";

const id = "0000000000000000000000a1";

const fake = (respond: () => Response) => {
  const urls: Array<string> = [];
  const http = HttpClient.make((request) => {
    urls.push(request.url);
    return Effect.succeed(HttpClientResponse.fromWeb(request, respond()));
  });
  return { urls, client: makeClient({ token: Effect.succeed("test"), http }) };
};

test("codemagic_start sends the request once when Codemagic fails", async () => {
  const { urls, client } = fake(() => new Response("busy", { status: 503 }));
  await Effect.runPromise(
    makeStartTool(client)
      .run({ appId: id, workflowId: "review", branch: "main" })
      .pipe(Effect.flip),
  );
  expect(urls).toHaveLength(1);
});
