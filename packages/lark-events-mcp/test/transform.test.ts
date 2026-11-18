import { Effect } from "effect";
import { expect, test } from "vitest";
import { makeTransform } from "../src/transform.ts";

const event = {
  eventId: "id",
  timestamp: "time",
  timestampOrigin: "local",
  data: { text: "hello" },
};
const run = (source: string) => Effect.runPromise(makeTransform()(source, { event }));

test("uses native results and JSON event input", async () => {
  expect(await run("input.event.data.text + input.event.eventId")).toBe("helloid");
  for (const source of ["return null", "return undefined", "const x = 1", "return"]) {
    expect(await run(source)).toBeNull();
  }
});

test("rejects invalid results, errors, truncation and host capabilities", async () => {
  for (const source of [
    "",
    "return {}",
    'throw new Error("failure")',
    'return "a".repeat(20000)',
    "return process.env",
    'return require("fs")',
    'return fetch("https://example.com")',
    "return tools.example({})",
  ]) {
    await expect(run(source)).rejects.toBeDefined();
  }
  await expect(Effect.runPromise(makeTransform()("while (true) {}", event))).rejects.toBeDefined();
});

test("enforces input and serialized output limits", async () => {
  const transform = makeTransform();
  expect(await Effect.runPromise(transform('return "ok"', { event: "a".repeat(1048564) }))).toBe(
    "ok",
  );
  await expect(
    Effect.runPromise(transform('return "ok"', { event: "a".repeat(1048565) })),
  ).rejects.toBeDefined();
  await expect(
    Effect.runPromise(makeTransform()('return "\\n".repeat(8192)', event)),
  ).rejects.toBeDefined();
});
