import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { DecisionModel } from "../../src/decision/decision-model.ts";
import { makeGetTool } from "../../src/lark/get.ts";
import { makeRunCode, makeWatchState } from "../../src/code/code.ts";
import { SourceName } from "../../src/source/contract.ts";
import { connect, unusedDecide } from "../support.ts";

const questions = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
];
const answer = { answers: [{ type: "predicate", name: "needs_me", probability: 0.9 }] };

test("a watch's code wakes with what decide answers", async () => {
  const calls: Array<DecisionModel.DecideInput> = [];
  const h = await connect({
    harness: "claude",
    decide: (input) => {
      calls.push(input);
      return Effect.succeed(answer);
    },
  });
  try {
    await h.watch(
      "first",
      `if (input.event.eventId === "drop") return null; const { answers } = await tools.events.decide({ questions: ${JSON.stringify(questions)}, input: [{ type: "json", value: { parent: "memes" } }] }); return answers[0].name`,
    );
    await h.watch("second", 'return "plain"');
    await h.publish({ eventId: "drop", payload: {} });
    await h.publish({ eventId: "sentinel", payload: { text: "hello" } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(3));
    expect(calls).toEqual([
      {
        questions,
        input: [{ type: "json", value: { parent: "memes" } }],
      },
    ]);
    expect(h.channel.map((wake) => wake.content).sort()).toEqual(["needs_me", "plain", "plain"]);
  } finally {
    await h.close();
  }
});

test("Lark failure stops the watch without leaking content to stderr", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const h = await connect({
    harness: "claude",
    request: async () => {
      throw { response: { data: { code: 99991672, msg: "fake secret scope message" } } };
    },
  });
  try {
    await h.watch("first", 'return await tools.lark.get({path:"/im/v1/messages"})');
    await h.publish({ eventId: "scope-event", payload: { text: "private message" } });
    await vi.waitFor(() => expect(h.channel).toHaveLength(1), { timeout: 3000 });
    expect(h.channel[0]?.content).toContain(
      "code failed on scope-event: ToolFailure: Lark API 99991672: fake secret scope message",
    );
    const output = JSON.stringify(log.mock.calls);
    expect(output).toContain('"eventId":"scope-event"');
    expect(output).not.toContain("fake secret");
    expect(output).not.toContain("private message");
  } finally {
    await h.close();
    log.mockRestore();
  }
});

test("timeout aborts an in-flight Lark request", async () => {
  const started = Promise.withResolvers<void>();
  const aborted = Promise.withResolvers<void>();
  const h = await connect({
    harness: "claude",
    eventTimeoutSeconds: 1,
    request: (_input, signal) =>
      new Promise((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => {
            aborted.resolve();
            reject(new Error("aborted"));
          },
          { once: true },
        );
        started.resolve();
      }),
  });
  try {
    await h.watch("first", 'return await tools.lark.get({path:"/im/v1/messages"})');
    await h.publish({ eventId: "blocked-request", payload: {} });
    await started.promise;
    await vi.waitFor(() => expect(h.channel).toHaveLength(1), { timeout: 3000 });
    expect(h.channel[0]?.content).toBe(
      "Watch first ended: code failed on blocked-request: TimeoutError: Operation timed out after '1s'",
    );
    await Promise.race([
      aborted.promise,
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("request was not aborted")), 2000),
      ),
    ]);
  } finally {
    await h.close();
  }
});

test("Lark GET rejects unsafe paths", async () => {
  const request = vi.fn(async () => ({ code: 0, msg: "ok", data: { parent: "memes" } }));
  const run = (code: string) =>
    makeRunCode({
      code,
      sources: [
        {
          name: Schema.decodeUnknownSync(SourceName)("lark"),
          tools: {
            get: makeGetTool({
              credentials: () =>
                Effect.succeed({ appId: "cli_0123456789abcdef", appSecret: "offline", request }),
            }),
          },
          events: [],
          instructions: "",
        },
      ],
      state: Effect.runSync(makeWatchState),
      decide: unusedDecide,
    });
  for (const path of [
    "/im/../users",
    "/im//users",
    "/im/%2e",
    "/im/\\users",
    "/im?x=1",
    "/im#x",
    "https://evil.test",
  ]) {
    await expect(
      Effect.runPromise(
        run(`return await tools.lark.get({ path: ${JSON.stringify(path)} })`)({
          eventId: "event",
          timestamp: new Date().toISOString(),
          data: {},
        }),
      ),
    ).rejects.toBeDefined();
  }
  expect(request).not.toHaveBeenCalled();
});
