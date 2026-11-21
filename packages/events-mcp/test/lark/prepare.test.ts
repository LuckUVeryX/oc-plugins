import { Effect, Option, Schema } from "effect";
import { expect, test, vi } from "vitest";
import { Decisions } from "../../src/core/decisions.ts";
import { makeGetTool } from "../../src/lark/get.ts";
import { makePrepare, makeWatchState } from "../../src/core/prepare.ts";
import { SourceName } from "../../src/core/source.ts";
import { connect } from "../harness.ts";

const questions = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
];
const answer = { answers: [{ type: "predicate", name: "needs_me", probability: 0.9 }] };
const pass = {
  prepare: "return input.event.data",
  transform: "return JSON.stringify(input.prepared)",
};

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

test("decide answers reach transform through input.prepared", async () => {
  const calls: Array<Decisions.DecideInput> = [];
  const h = await connect({
    decide: (input) => {
      calls.push(input);
      return Effect.succeed(answer);
    },
  });
  try {
    h.stream({
      watchId: "prepared",
      prepare: `if (input.event.eventId === "drop") return null; return await tools.events.decide({ questions: ${JSON.stringify(questions)}, input: [{ type: "json", value: { parent: "memes" } }] })`,
      transform: "return input.prepared.answers[0].name",
    });
    h.stream({
      watchId: "without-decide-call",
      prepare: 'return { text: "prepared" }',
      transform: "return input.prepared.text",
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
      expect.arrayContaining(["needs_me", "prepared"]),
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
      prepare: 'return await tools.lark.get({path:"/im/v1/messages"})',
      transform: pass.transform,
    });
    await active(h);
    await h.publish({ eventId: "scope-event", payload: { text: "private message" } });
    expect(await terminated(h)).toContain(
      "prepare failed on scope-event: ToolFailure: Lark API 99991672: fake secret scope message",
    );
    expect((await h.call("list_watches")).watches).toEqual([]);
    const output = JSON.stringify(log.mock.calls);
    expect(output).toContain('"step":"prepare"');
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
      prepare: 'return await tools.lark.get({path:"/im/v1/messages"})',
      transform: pass.transform,
    });
    await active(h);
    await h.publish({ eventId: "blocked-request", payload: {} });
    await started.promise;
    expect(await terminated(h)).toBe(
      "prepare failed on blocked-request: TimeoutError: Operation timed out after '1s'",
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
  const prepare = makePrepare({
    sources: [
      {
        name: Schema.decodeUnknownSync(SourceName)("lark"),
        tools: { get: makeGetTool(Effect.succeed(request)) },
        events: [],
        instructions: "",
      },
    ],
    state: await Effect.runPromise(makeWatchState),
    decide: Option.none(),
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
        prepare(`return await tools.lark.get({ path: ${JSON.stringify(path)} })`, {
          eventId: "event",
          timestamp: new Date().toISOString(),
          data: {},
        }),
      ),
    ).rejects.toBeDefined();
  }
  expect(request).not.toHaveBeenCalled();
});
