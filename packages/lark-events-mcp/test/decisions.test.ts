import { Generate } from "@opencode/schema/generate";
import { ConfigProvider, Effect, Schema } from "effect";
import { afterAll, expect, test, vi } from "vitest";
import { decisionModel } from "../src/config.ts";
import { makeDecisions, type Decisions } from "../src/decisions.ts";
import { connect } from "./harness.ts";

const originalFetch = globalThis.fetch;
let observeAbort = () => {};
globalThis.fetch = Object.assign(
  (url: Parameters<typeof fetch>[0], init?: Parameters<typeof fetch>[1]) => {
    init?.signal?.addEventListener("abort", () => observeAbort(), { once: true });
    return originalFetch(url, init);
  },
  { preconnect: originalFetch.preconnect },
);
afterAll(() => {
  globalThis.fetch = originalFetch;
});

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

test("rejects decisions when no model is configured", async () => {
  const h = await connect();
  try {
    const request = h.stream({
      watchId: "unconfigured",
      prepare: "return input.event.data",
      transform: "return null",
      decisions: { questions },
    });
    expect(await request.done).toMatchObject({
      code: -32602,
      message: expect.stringContaining("the operator has not configured a model"),
    });
  } finally {
    await h.close();
  }
});

test("operator model configuration rejects invalid explicit values", async () => {
  expect(
    (await Effect.runPromise(decisionModel.parse(ConfigProvider.fromEnvRecord({}))))._tag,
  ).toBe("None");
  expect(
    (
      await Effect.runPromise(
        decisionModel.parse(
          ConfigProvider.fromEnvRecord({ LARK_DECISIONS_MODEL: JSON.stringify(model) }),
        ),
      )
    )._tag,
  ).toBe("Some");
  await expect(
    Effect.runPromise(
      decisionModel.parse(ConfigProvider.fromEnvRecord({ LARK_DECISIONS_MODEL: "bad" })),
    ),
  ).rejects.toBeDefined();
});

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

test("heartbeat continues during a held request and expiry discards queued events", async () => {
  const held = Promise.withResolvers<void>();
  const released = Promise.withResolvers<void>();
  const aborted = Promise.withResolvers<void>();
  observeAbort = () => aborted.resolve();
  let calls = 0;
  const http = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch() {
      calls++;
      held.resolve();
      await released.promise;
      return Response.json({ data: answer });
    },
  });
  const h = await connect({
    maxSeconds: 22,
    decide: makeDecisions(model, async () => ({ url: http.url.toString(), auth: undefined })),
  });
  try {
    h.stream({
      prepare: "return input.event.data",
      watchId: "expiry",
      decisions: { questions },
      transform: 'return "bad"',
    });
    await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "active")).toBe(true));
    const publish = (eventId: string) => h.publish({ eventId, payload: {} });
    await publish("first");
    await Promise.race([
      held.promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error("no HTTP")), 2000)),
    ]);
    await publish("queued");
    await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "heartbeat")).toBe(true), {
      timeout: 25_000,
    });
    await Promise.race([
      vi.waitFor(() => expect(h.notifications.some((n) => n.type === "terminated")).toBe(true), {
        timeout: 3000,
      }),
      new Promise((_, reject) => setTimeout(() => reject(new Error("no termination")), 3000)),
    ]);
    await Promise.race([
      aborted.promise,
      new Promise((_, reject) => setTimeout(() => reject(new Error("no abort")), 3000)),
    ]);
    released.resolve();
    expect(calls).toBe(1);
    expect(h.notifications.filter((n) => n.type === "event")).toEqual([]);
  } finally {
    released.resolve();
    await h.close();
    http.stop(true);
  }
});
