import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { makeDecisions, type Decisions } from "../src/decisions.ts";
import { connect } from "./harness.ts";

const questions: Decisions["questions"] = [
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

test("decision client sends prepared input and preserves model failures", async () => {
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
    const result = await Effect.runPromise(decide({ questions }, { context: "parent" }));
    expect(result.answers).toEqual(answer.answers);
    expect(bodies).toContainEqual({
      model,
      questions,
      input: [{ type: "json", value: { context: "parent" } }],
    });
    fail = true;
    await expect(Effect.runPromise(decide({ questions }, {}))).rejects.toThrow("provider down");
    const h = await connect({ decide });
    try {
      h.stream({
        watchId: "model-failure",
        prepare: "return input.event.data",
        transform: 'return "ok"',
        decisions: { questions },
      });
      await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "active")).toBe(true));
      await h.publish({ eventId: "model-event", payload: {} });
      await vi.waitFor(() =>
        expect(h.notifications.some((n) => n.type === "terminated")).toBe(true),
      );
      expect(h.notifications.find((n) => n.type === "terminated")?.reason).toContain(
        "decisions failed on model-event:",
      );
      expect(h.notifications.find((n) => n.type === "terminated")?.reason).toContain(
        "provider down",
      );
    } finally {
      await h.close();
    }
  } finally {
    http.stop(true);
  }
});
