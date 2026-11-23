import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { makeDecisions, type DecideInput } from "../../src/decision/decision-model.ts";
import { connect } from "../support.ts";

const questions: DecideInput["questions"] = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
  { type: "choice", name: "c", instructions: "test", choices: [{ value: true }] },
  { type: "score", name: "s", instructions: "test", levels: [{ label: "high" }] },
];
const model = Schema.decodeUnknownSync(Generate.DecisionRequest.fields.model)({
  providerID: "fake",
  id: "test",
  variant: "v",
});
const answer = {
  answers: [
    { type: "error", name: "p", reason: "fixture" },
    {
      type: "choice",
      name: "c",
      choice: true,
      probabilities: [{ value: true, probability: 1 }],
      confidence: 1,
    },
    {
      type: "score",
      name: "s",
      score: 0,
      probabilities: [{ value: 0, label: "high", probability: 1 }],
      confidence: 1,
    },
    { type: "predicate", name: "extra", probability: 0.5 },
  ],
  tokens: { input: 3, output: 1 },
  native: { fixture: true },
};

test("decide sends its input and surfaces model failures through the code and the tool", async () => {
  const bodies: unknown[] = [];
  let fail = false;
  const http = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      bodies.push(await request.json());
      if (fail) return new Response("provider down", { status: 503 });
      return Response.json({ data: answer });
    },
  });
  try {
    const decide = makeDecisions(model, async () => ({
      url: http.url.toString(),
      auth: undefined,
    }));
    const context = [{ type: "text" as const, text: "I am Pat." }];
    const result = await Effect.runPromise(decide({ questions, input: context }));
    expect(result).toMatchObject({ answers: answer.answers });
    expect(bodies).toContainEqual({ model, questions, input: context });
    fail = true;
    await expect(Effect.runPromise(decide({ questions }))).rejects.toThrow("provider down");
    const h = await connect({ decide, harness: "claude" });
    try {
      await h.watch(
        "model-failure",
        `await tools.events.decide({ questions: ${JSON.stringify(questions)} }); return "ok"`,
      );
      h.publish({ eventId: "model-event", payload: {} });
      await vi.waitFor(() => expect(h.channel).toHaveLength(1));
      expect(h.channel[0]?.content).toContain("code failed on model-event:");
      expect(h.channel[0]?.content).toContain("provider down");
    } finally {
      await h.close();
    }
    fail = false;
    const opencode = await connect({ decide });
    try {
      expect(await opencode.call("decide", { questions, input: context })).toMatchObject({
        answers: answer.answers,
      });
      fail = true;
      const failed = await opencode.callRaw("decide", { questions });
      expect(failed.isError).toBe(true);
      expect(failed.text).toContain("provider down");
    } finally {
      await opencode.close();
    }
  } finally {
    http.stop(true);
  }
});
