import { Client } from "@modelcontextprotocol/client";
import { getDefaultEnvironment, StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

// Claude Code delivers channel notifications only on a pre-2026-07-28 connection.
test("a legacy client connects", async () => {
  const dir = mkdtempSync(join(tmpdir(), "events-legacy-"));
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL("../../src/main.ts", import.meta.url)), "opencode"],
    env: { ...getDefaultEnvironment(), EVENTS_MCP_HUB_DIR: dir },
    stderr: "pipe",
  });
  const client = new Client(
    { name: "legacy", version: "0" },
    { versionNegotiation: { mode: "legacy" } },
  );
  try {
    await client.connect(transport);
    expect(client.getNegotiatedProtocolVersion()).toBe("2025-11-25");
    expect((await client.listTools()).tools).toHaveLength(3);
  } finally {
    await client.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
