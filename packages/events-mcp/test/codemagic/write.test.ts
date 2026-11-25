import { expect, test } from "vitest";
import { Effect } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeClient } from "../../src/codemagic/client.ts";
import { makeArtifactsTool, makeCancelTool, makeStartTool } from "../../src/codemagic/tools.ts";

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

test("codemagic_cancel reports only a 204 as canceled", async () => {
  const cancel = (status: number) =>
    Effect.runPromise(
      makeCancelTool(fake(() => new Response(null, { status })).client)
        .run({ buildId: id })
        .pipe(Effect.orElseSucceed(() => "failed")),
    );
  expect(await cancel(204)).toEqual({ buildId: id, canceled: true });
  expect(await cancel(409)).toEqual({ buildId: id, canceled: false });
  // Codemagic answers a missing build with its web app's HTML page and status 200.
  expect(await cancel(200)).toBe("failed");
});

test("codemagic_artifacts never sends the token to another host", async () => {
  const artefacts = [{ name: "app.apk", url: "https://api.codemagic.io.evil.test/artifacts/1" }];
  const { urls, client } = fake(() => Response.json({ build: { buildActions: [], artefacts } }));
  await Effect.runPromise(makeArtifactsTool(client).run({ buildId: id }).pipe(Effect.flip));
  expect(urls).toEqual([`https://api.codemagic.io/builds/${id}`]);
});
