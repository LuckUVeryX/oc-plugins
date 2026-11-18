import { Effect } from "effect";
import { expect, test, vi } from "vitest";
import { makePrepare } from "../src/prepare.ts";
import { connect } from "./harness.ts";

const questions = [
  { type: "predicate", name: "p", instructions: "test", criteria: { true: "yes" } },
];
const answer = { answers: [], tokens: { input: 0, output: 0 }, native: {} };
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

test("prepare feeds decisions and transform, and null skips decisions", async () => {
  const values: unknown[] = [];
  const h = await connect({
    decide: (_decisions, value) => {
      values.push(value);
      return Effect.succeed(answer);
    },
  });
  try {
    h.stream({
      watchId: "prepared",
      decisions: { questions },
      prepare: 'if (input.event.eventId === "drop") return null; return { parent: "memes" }',
      transform: "return input.prepared.parent",
    });
    h.stream({
      watchId: "without-decisions",
      prepare: 'return { text: "prepared" }',
      transform: "return input.prepared.text",
    });
    await active(h, 2);
    await h.publish({ eventId: "drop", payload: {} });
    await h.publish({ eventId: "sentinel", payload: { text: "hello" } });
    await vi.waitFor(() =>
      expect(h.notifications.filter((n) => n.eventId === "sentinel")).toHaveLength(2),
    );
    expect(values).toEqual([{ parent: "memes" }]);
    expect(h.notifications.filter((n) => n.eventId === "sentinel").map((n) => n.data)).toEqual(
      expect.arrayContaining(["memes", "prepared"]),
    );
  } finally {
    await h.close();
  }
});

test("Lark failure stops the watch without leaking content to stderr", async () => {
  const log = vi.spyOn(console, "error").mockImplementation(() => {});
  const h = await connect({
    larkRequest: async () => {
      throw { response: { data: { code: 99991672, msg: "fake secret scope message" } } };
    },
  });
  try {
    h.stream({
      watchId: "scope",
      prepare: 'return await tools["lark-events"].get({path:"/im/v1/messages"})',
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
    larkRequest: (_input, signal) =>
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
      prepare: 'return await tools["lark-events"].get({path:"/im/v1/messages"})',
      transform: pass.transform,
    });
    await active(h);
    await h.publish({ eventId: "blocked-request", payload: {} });
    await started.promise;
    expect(await terminated(h)).toContain("prepare failed on blocked-request:");
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
  const prepare = makePrepare(request);
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
        prepare(`return await tools["lark-events"].get({ path: ${JSON.stringify(path)} })`, {}),
      ),
    ).rejects.toBeDefined();
  }
  expect(request).not.toHaveBeenCalled();
});
