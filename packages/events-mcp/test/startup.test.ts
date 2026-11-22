import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const main = fileURLToPath(new URL("../src/main.ts", import.meta.url));

const start = (...args: ReadonlyArray<string>) =>
  spawnSync(process.execPath, [main, ...args], { encoding: "utf8" });

test("a missing harness argument stops at startup", () => {
  const result = start();
  expect(result.status).toBe(1);
  expect(result.stderr).toContain("no harness given. Use one of: opencode, claude.");
});

test("an unknown harness argument stops at startup", () => {
  const result = start("bogus");
  expect(result.status).toBe(1);
  expect(result.stderr).toContain('unknown harness "bogus"');
});
