import { fromJsonSchema } from "@modelcontextprotocol/server";
import { Generate } from "@opencode/schema/generate";
import { ConfigProvider, Effect, JsonSchema, Schema, SchemaRepresentation } from "effect";
import { afterAll, expect, test, vi } from "vitest";
import { decisionModel } from "../src/config.ts";
import { decodeArguments } from "../src/contract.ts";
import { makeDecisions } from "../src/decisions.ts";
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

const questions = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
  { type: "choice", name: "c", instructions: "test", choices: [{ value: true }] },
  { type: "score", name: "s", instructions: "test", levels: [{ label: "high" }] },
];
const input = [
  { type: "text", text: "test" },
  { type: "json", value: { nested: [1, true, null, { model: "evidence" }] } },
  { type: "file", data: "ZmFrZQ==", mime: "text/plain", name: "test" },
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

test("native requests pass through MCP and stop aborts pending local HTTP", async () => {
  const bodies: unknown[] = [];
  const held = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const aborted = Promise.withResolvers<void>();
  observeAbort = () => aborted.resolve();
  let auth = "";
  const http = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    async fetch(request) {
      expect(new URL(request.url).pathname).toBe("/api/experimental/generate/decisions");
      auth = request.headers.get("authorization") ?? "";
      const body = await request.json();
      bodies.push(body);
      const id = body.input.at(-1).value.eventId;
      if (
        id === "hold" &&
        !body.input.some((part: { text?: string }) => part.text === "second-watch")
      ) {
        held.resolve();
        await release.promise;
      }
      if (id === "failure") return new Response("fixture error", { status: 500 });
      return Response.json({ data: answer });
    },
  });
  const decide = makeDecisions(model, async () => ({
    url: http.url.toString(),
    auth: { type: "basic", username: "fake", password: "secret" },
  }));
  const h = await connect({ decide });
  const client = h.client;
  const stream = (args: Record<string, unknown>) => h.stream(args);
  const publish = (eventId: string, chat = "match") =>
    h.publish({ eventId, payload: { message: { chat_id: chat } } });
  const active = () => h.notifications.filter((n) => n.type === "active").map((n) => n.id);
  const events = () => h.notifications.filter((n) => n.type === "event");
  try {
    const listing = await client.request(
      { method: "events/list", params: {} },
      Schema.toStandardSchemaV1(
        Schema.Struct({
          events: Schema.Array(
            Schema.Struct({
              name: Schema.String,
              inputSchema: Schema.Record(Schema.String, Schema.Unknown),
            }),
          ),
        }),
      ),
    );
    const document = listing.events.find(
      (event) => event.name === "im.message.receive_v1",
    )?.inputSchema;
    expect(document).toBeDefined();
    const advertised = fromJsonSchema(document!);
    const codec = SchemaRepresentation.fromJsonSchemaDocument(
      JsonSchema.fromSchemaDraft2020_12(document!),
      { patterns: "apply" },
    );
    expect(
      Schema.decodeUnknownSync(codec as Schema.Codec<unknown>)({
        watchId: "schema",
        transform: "return null",
        decisions: { questions, input },
      }),
    ).toBeDefined();
    expect(
      await advertised["~standard"].validate({
        watchId: "schema",
        transform: "return null",
        decisions: { questions, input },
      }),
    ).toHaveProperty("value");
    expect(() =>
      Schema.decodeUnknownSync(codec as Schema.Codec<unknown>)({
        watchId: "schema",
        transform: "return null",
        decisions: { questions: [{ type: "file" }] },
      }),
    ).toThrow();
    expect(
      await advertised["~standard"].validate({
        watchId: "schema",
        transform: "return null",
        decisions: { questions: [{ ...questions[0], model: "bad" }] },
      }),
    ).toHaveProperty("issues");
    expect(
      await advertised["~standard"].validate({
        watchId: "schema",
        transform: "return null",
        decisions: { questions, input: [{ type: "file", data: "not base64!", mime: "x" }] },
      }),
    ).toHaveProperty("issues");
    expect(document).toHaveProperty(
      ["properties", "decisions", "description"],
      expect.stringContaining("Requires transform."),
    );
    for (const [input, message] of [
      [null, "Expected object"],
      [{}, 'Missing key\n  at ["watchId"]'],
      [{ watchId: "x", extra: true }, 'Expected no excess property\n  at ["extra"]'],
      [{ watchId: "x", filter: [] }, 'Expected object\n  at ["filter"]'],
    ] as const) {
      expect(await Effect.runPromise(Effect.flip(decodeArguments({}, input)))).toBe(message);
    }
    expect(
      await Effect.runPromise(
        Effect.flip(decodeArguments({}, { watchId: "x", decisions: { questions } })),
      ),
    ).toBe("Decisions requires transform; add transform or omit decisions");
    expect(
      await Effect.runPromise(
        Effect.flip(
          decodeArguments(
            {},
            {
              watchId: "x",
              transform: "return null",
              decisions: { questions, input: [{ type: "text", text: "x", model: "override" }] },
            },
          ),
        ),
      ),
    ).toContain("Expected no excess property");
    await expect(
      client.request(
        {
          method: "events/stream",
          params: {
            name: "im.message.receive_v1",
            arguments: { watchId: "missing-transform", decisions: { questions } },
          },
        },
        Schema.toStandardSchemaV1(Schema.Unknown),
      ),
    ).rejects.toMatchObject({ code: -32602 });
    expect(
      await Effect.runPromise(
        Effect.flip(
          decodeArguments(
            {},
            {
              watchId: "x",
              transform: "return null",
              decisions: { questions: [{ ...questions[0], model: "bad" }] },
            },
          ),
        ),
      ),
    ).toContain("Expected no excess property");
    stream({ watchId: "raw" });
    stream({ watchId: "transform", transform: 'return "plain"' });
    stream({
      watchId: "decision",
      filter: { "message.chat_id": "match" },
      decisions: { questions, input },
      transform:
        'if (input.event.eventId === "suppress") return null; return input.event.eventId + ":" + input.result.answers[0].reason + ":" + input.result.answers[1].type + ":" + input.result.tokens.input',
    });
    stream({
      watchId: "decision-2",
      filter: { "message.chat_id": "match" },
      decisions: { questions, input: [...input, { type: "text", text: "second-watch" }] },
      transform: "return input.event.eventId",
    });
    await vi.waitFor(() => expect(active()).toHaveLength(4));
    await publish("filtered", "other");
    await publish("one");
    await publish("suppress");
    await publish("failure");
    await publish("two");
    await vi.waitFor(() => {
      expect(events().some((event) => event.id === active()[2] && event.eventId === "two")).toBe(
        true,
      );
      expect(events().some((event) => event.id === active()[3] && event.eventId === "two")).toBe(
        true,
      );
    });
    expect(bodies).toHaveLength(8);
    expect(auth).toBe(`Basic ${btoa("fake:secret")}`);
    expect(bodies).toContainEqual({
      model,
      questions,
      input: [
        ...input,
        {
          type: "json",
          value: {
            eventId: "one",
            timestamp: "2026-01-01T00:00:00.000Z",
            timestampOrigin: "upstream",
            data: { message: { chat_id: "match" } },
          },
        },
      ],
    });
    expect(
      events()
        .filter((event) => event.id === active()[2])
        .map((event) => event.data),
    ).toEqual(["one:fixture:choice:3", "two:fixture:choice:3"]);
    expect(
      events()
        .filter((event) => event.id === active()[3])
        .map((event) => event.data),
    ).toEqual(["one", "suppress", "two"]);
    await publish("hold");
    await held.promise;
    await vi.waitFor(() =>
      expect(events().some((event) => event.id === active()[3] && event.eventId === "hold")).toBe(
        true,
      ),
    );
    await client.request(
      {
        method: "tools/call",
        params: { name: "stop_watch", arguments: { watchId: "decision-2" } },
      },
      Schema.toStandardSchemaV1(Schema.Unknown),
    );
    await publish("queued");
    await client.request(
      { method: "tools/call", params: { name: "stop_watch", arguments: { watchId: "decision" } } },
      Schema.toStandardSchemaV1(Schema.Unknown),
    );
    await aborted.promise;
    release.resolve();
    expect(bodies).toHaveLength(10);
    expect(events().filter((event) => event.id === active()[2])).toHaveLength(2);
  } finally {
    release.resolve();
    await h.close();
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
    h.stream({ watchId: "expiry", decisions: { questions }, transform: 'return "bad"' });
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
