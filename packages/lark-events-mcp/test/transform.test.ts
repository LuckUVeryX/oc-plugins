import { ConfigProvider, Effect } from "effect";
import { expect, test } from "vitest";
import { transformConfig } from "../src/config.ts";
import { makeTransform } from "../src/transform.ts";

const limits = Effect.runSync(transformConfig.parse(ConfigProvider.fromEnvRecord({})));
const event = {
  eventId: "id",
  timestamp: "time",
  timestampOrigin: "local",
  data: { text: "hello" },
};
const run = (source: string) => Effect.runPromise(makeTransform(limits)(source, event));

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
  await expect(
    Effect.runPromise(makeTransform({ ...limits, timeoutMs: 10 })("while (true) {}", event)),
  ).rejects.toBeDefined();
});

test("enforces input and serialized output limits", async () => {
  await expect(
    Effect.runPromise(makeTransform({ ...limits, inputBytes: 1 })('return "ok"', event)),
  ).rejects.toBeDefined();
  await expect(
    Effect.runPromise(
      makeTransform({ ...limits, outputBytes: 10 })('return "\\n\\n\\n\\n\\n"', event),
    ),
  ).rejects.toBeDefined();
});
