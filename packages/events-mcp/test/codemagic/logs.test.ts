import { expect, test } from "vitest";
import { Effect } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { makeClient } from "../../src/codemagic/client.ts";
import { makeLogsTool } from "../../src/codemagic/tools.ts";

const buildId = "0000000000000000000000b1";
const stepId = "0000000000000000000000c1";

const readLog = (logs: Readonly<Record<string, string>>, ran = true) => {
  const api = "https://api.codemagic.io";
  const step = { _id: stepId, name: "Build", status: ran ? "failed" : null };
  const subactions = (ran ? ["s1", "s2"] : []).map((id) => ({
    logUrl: `${api}/builds/${buildId}/step/${id}`,
  }));
  const http = HttpClient.make((request) => {
    const log = logs[request.url.slice(request.url.lastIndexOf("/") + 1)];
    const response = request.url.endsWith(`/builds/${buildId}`)
      ? Response.json({
          build: {
            finishedAt: "2026-01-01T00:00:00Z",
            buildActions: [{ ...step, logUrl: null, subactions }],
          },
        })
      : new Response(log ?? "{}", { status: log === undefined ? 404 : 200 });
    return Effect.succeed(HttpClientResponse.fromWeb(request, response));
  });
  return Effect.runPromise(
    makeLogsTool(makeClient({ token: Effect.succeed("test"), http })).run({ buildId, stepId }),
  );
};

test("codemagic_logs joins a script step's subactions and keeps the end", async () => {
  expect(await readLog({ s1: "x".repeat(60_000), s2: "the error" })).toMatchObject({
    complete: true,
    truncated: true,
    log: expect.stringMatching(/x\nthe error$/),
  });
});

test("codemagic_logs does not call a partly missing log complete", async () => {
  expect(await readLog({ s1: "first" })).toMatchObject({ complete: false, log: "first" });
});

test("codemagic_logs is complete for a step that never ran in an ended build", async () => {
  expect(await readLog({}, false)).toMatchObject({ complete: true, log: "" });
});
