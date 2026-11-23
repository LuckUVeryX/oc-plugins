import { Effect, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { DecisionModel } from "../../src/decision/decision-model.ts";
import { makeGetTool } from "../../src/lark/get.ts";
import { makeRunScript, makeWatchState } from "../../src/script/script.ts";
import { IntegrationName } from "../../src/integration/contract.ts";
import { connect, unusedDecide } from "../support.ts";

const questions = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
];
const answer = { answers: [{ type: "predicate", name: "needs_me", probability: 0.9 }] };

async function active(h: Awaited<ReturnType<typeof connect>>, count = 1) {
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(count),
  );
}
async function terminated(h: Awaited<ReturnType<typeof connect>>) {
  await vi.waitFor(() => expect(h.notifications.some((n) => n.type === "terminated")).toBe(true), {
    timeout: 3000,
  });
  return h.notifications.find((n) => n.type === "terminated")?.reason;
}

test("a script wakes with what decide answers", async () => {
  const calls: Array<DecisionModel.DecideInput> = [];
  const h = await connect({
    decide: (input) => {
      calls.push(input);
      return Effect.succeed(answer);
    },
  });
  try {
    h.stream({
      watchId: "decided",
      script: `if (input.event.eventId === "drop") return null; const { answers } = await tools.events.decide({ questions: ${JSON.stringify(questions)}, input: [{ type: "json", value: { parent: "memes" } }] }); return answers[0].name`,
    });
    h.stream({
      watchId: "without-decide-call",
      script: 'return "plain"',
    });
    await active(h, 2);
    await h.publish({ eventId: "drop", payload: {} });
    await h.publish({ eventId: "sentinel", payload: { text: "hello" } });
    await vi.waitFor(() =>
      expect(h.notifications.filter((n) => n.eventId === "sentinel")).toHaveLength(2),
    );
    expect(calls).toEqual([
      {
        questions,
        input: [{ type: "json", value: { parent: "memes" } }],
      },
    ]);
    expect(h.notifications.filter((n) => n.eventId === "sentinel").map((n) => n.data)).toEqual(
      expect.arrayContaining(["needs_me", "plain"]),
    );
  } finally {
    await h.close();
  }
});

test("Lark failure stops the watch without leaking content to stderr", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const h = await connect({
    request: async () => {
      throw { response: { data: { code: 99991672, msg: "fake secret scope message" } } };
    },
  });
  try {
    h.stream({
      watchId: "scope",
      script: 'return await tools.lark.get({path:"/im/v1/messages"})',
    });
    await active(h);
    await h.publish({ eventId: "scope-event", payload: { text: "private message" } });
    expect(await terminated(h)).toContain(
      "script failed on scope-event: ToolFailure: Lark API 99991672: fake secret scope message",
    );
    expect((await h.call("watches")).watches).toEqual([]);
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
    h.stream({
      watchId: "abort-timeout",
      script: 'return await tools.lark.get({path:"/im/v1/messages"})',
    });
    await active(h);
    await h.publish({ eventId: "blocked-request", payload: {} });
    await started.promise;
    expect(await terminated(h)).toBe(
      "script failed on blocked-request: TimeoutError: Operation timed out after '1s'",
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
  const script = (code: string) =>
    makeRunScript({
      code,
      integrations: [
        {
          name: Schema.decodeUnknownSync(IntegrationName)("lark"),
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
        script(`return await tools.lark.get({ path: ${JSON.stringify(path)} })`)({
          eventId: "event",
          timestamp: new Date().toISOString(),
          data: {},
        }),
      ),
    ).rejects.toBeDefined();
  }
  expect(request).not.toHaveBeenCalled();
});
