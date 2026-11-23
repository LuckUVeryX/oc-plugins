import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, onTestFinished, test } from "vitest";

const main = fileURLToPath(new URL("../../src/main.ts", import.meta.url));

const line = (value: unknown) => `${JSON.stringify(value)}\n`;

test("a broken stdout ends the connection while stdin stays open", async () => {
  const dir = mkdtempSync(join(tmpdir(), "events-transport-"));
  onTestFinished(() => rmSync(dir, { recursive: true, force: true }));
  const child = spawn(process.execPath, [main, "claude"], {
    stdio: ["pipe", "pipe", "pipe"],
    env: { ...process.env, EVENTS_MCP_HUB_DIR: dir },
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });
  child.stdin.write(
    line({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2025-11-25",
        capabilities: {},
        clientInfo: { name: "broken-pipe", version: "0" },
      },
    }),
  );
  const initialized = await Promise.race([
    new Promise<"ready">((resolve) => child.stdout.once("data", () => resolve("ready"))),
    new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 5_000)),
  ]);
  if (initialized === "timeout") {
    child.kill("SIGKILL");
    throw new Error(`no initialize response\n${stderr}`);
  }

  child.stdout.destroy();
  child.stdin.write(line({ jsonrpc: "2.0", id: 2, method: "tools/list", params: {} }));

  const exited = await Promise.race([
    new Promise<number | null>((resolve) => child.once("exit", (code) => resolve(code))),
    new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 5_000)),
  ]);
  if (exited === "timeout") child.kill("SIGKILL");
  expect(stderr).toContain("EPIPE");
  expect(exited).not.toBe("timeout");
});
