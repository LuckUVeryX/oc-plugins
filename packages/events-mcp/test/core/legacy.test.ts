import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

// Claude Code delivers channel notifications only on a pre-2026-07-28 connection.
test("a legacy client connects", async () => {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [fileURLToPath(new URL("../../src/main.ts", import.meta.url)), "opencode"],
    stderr: "pipe",
  });
  const client = new Client(
    { name: "legacy", version: "0" },
    { versionNegotiation: { mode: "legacy" } },
  );
  try {
    await client.connect(transport);
    expect(client.getNegotiatedProtocolVersion()).toBe("2025-11-25");
    expect((await client.listTools()).tools).toHaveLength(4);
  } finally {
    await client.close();
  }
});
