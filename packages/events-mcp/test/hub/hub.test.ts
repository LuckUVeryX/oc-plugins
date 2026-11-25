import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { Schema } from "effect";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, statSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const main = fileURLToPath(new URL("../../src/main.ts", import.meta.url));

const Tools = Schema.toStandardSchemaV1(
  Schema.Struct({ tools: Schema.Array(Schema.Struct({ name: Schema.String })) }),
);

const Events = Schema.toStandardSchemaV1(
  Schema.Struct({ events: Schema.Array(Schema.Struct({ name: Schema.String })) }),
);

const environment = (root: string) => {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env))
    if (value !== undefined && !key.startsWith("LARK_")) env[key] = value;
  delete env.GITLAB_TOKEN;
  delete env.CODEX_HOME;
  env["EVENTS_MCP_HUB_DIR"] = root;
  return env;
};

const directory = (root: string) => join(root, "run");

const session = async (
  root: string,
  harness: string,
  extra: Record<string, string> = {},
  mode: "auto" | "legacy" = "auto",
) => {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [main, harness],
    env: { ...environment(root), ...extra },
    stderr: "ignore",
  });
  const client = new Client({ name: "hub-test", version: "0" }, { versionNegotiation: { mode } });
  await client.connect(transport);
  return client;
};

const toolNames = async (client: Client) =>
  (await client.request({ method: "tools/list", params: {} }, Tools)).tools.map(
    (tool) => tool.name,
  );

const waitFor = async (check: () => boolean, timeoutMs = 10_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return check();
};

const socketExists = (root: string) => existsSync(join(directory(root), "hub.sock"));

// A proxy retries connect until the hub socket appears. The hub may accept that
// connection before its listen callback runs, so a connection made on arrival
// must still end in initialize.
const connectOnArrival = (path: string) =>
  new Promise<net.Socket>((resolve, reject) => {
    const deadline = Date.now() + 10_000;
    const attempt = () => {
      const socket = net.connect({ path });
      socket.once("connect", () => resolve(socket));
      socket.once("error", () => {
        socket.destroy();
        if (Date.now() > deadline) reject(new Error(`no hub answered at ${path}`));
        else setImmediate(attempt);
      });
    };
    attempt();
  });

const lines = (socket: net.Socket) => {
  let buffered = "";
  const waiters: Array<(line: string) => void> = [];
  socket.setEncoding("utf8");
  socket.on("data", (chunk: string) => {
    buffered += chunk;
    while (true) {
      const end = buffered.indexOf("\n");
      if (end < 0) return;
      const line = buffered.slice(0, end);
      buffered = buffered.slice(end + 1);
      waiters.shift()?.(line);
    }
  });
  return () =>
    new Promise<string>((resolve, reject) => {
      const end = buffered.indexOf("\n");
      if (end >= 0) {
        const line = buffered.slice(0, end);
        buffered = buffered.slice(end + 1);
        resolve(line);
        return;
      }
      const timer = setTimeout(() => reject(new Error("no line within 5000 ms")), 5_000);
      waiters.push((line) => {
        clearTimeout(timer);
        resolve(line);
      });
    });
};

const initializeOnArrival = async (path: string) => {
  const socket = await connectOnArrival(path);
  const next = lines(socket);
  try {
    socket.write('{"harness":"opencode","version":"0.0.0"}\n');
    const reply: unknown = JSON.parse(await next());
    socket.write(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "initialize",
        params: {
          protocolVersion: "2025-11-25",
          capabilities: {},
          clientInfo: { name: "cold-start", version: "0" },
        },
      }) + "\n",
    );
    const message: unknown = JSON.parse(await next());
    return { reply, message };
  } finally {
    socket.destroy();
  }
};

const cleanup = async (root: string) => {
  await waitFor(() => !socketExists(root));
  rmSync(root, { recursive: true, force: true });
};

test("two proxies started together share one hub", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  try {
    const clients = await Promise.all([session(root, "opencode"), session(root, "claude")]);
    try {
      expect(socketExists(root)).toBe(true);
      const log = readFileSync(join(root, "state", "hub.log"), "utf8");
      const listening = log.split("\n").filter((line) => line.includes(": listening at "));
      expect(listening).toHaveLength(1);
    } finally {
      await Promise.all(clients.map((client) => client.close()));
    }
  } finally {
    await cleanup(root);
  }
}, 30_000);

test("each connection gets the surface of its own harness", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  try {
    const [opencode, claude] = await Promise.all([
      session(root, "opencode"),
      session(root, "claude"),
    ]);
    try {
      expect(await toolNames(opencode)).not.toContain("watch");
      expect(await toolNames(opencode)).not.toContain("list_events");
      expect(await toolNames(claude)).toContain("watch");
      expect(await toolNames(claude)).toContain("list_events");
      const events = await opencode.request({ method: "events/list", params: {} }, Events);
      expect(events.events.length).toBeGreaterThan(0);
    } finally {
      await Promise.all([opencode.close(), claude.close()]);
    }
  } finally {
    await cleanup(root);
  }
}, 30_000);

test("the hub exits after its last connection closes", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  try {
    const opencode = await session(root, "opencode");
    await opencode.close();
    expect(await waitFor(() => !socketExists(root))).toBe(true);
  } finally {
    await cleanup(root);
  }
}, 30_000);

test("a hello-less connection does not keep an unserved hub alive", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  try {
    const proxy = spawn(process.execPath, [main, "opencode"], {
      env: environment(root),
      stdio: ["pipe", "ignore", "ignore"],
    });
    const pid = proxy.pid;
    if (pid === undefined) throw new Error("proxy did not start");
    expect(await waitFor(() => socketExists(root))).toBe(true);

    const socket = net.connect({ path: join(directory(root), "hub.sock") });
    await new Promise<void>((resolve, reject) => {
      socket.once("connect", () => resolve());
      socket.once("error", reject);
    });
    process.kill(pid, "SIGKILL");
    expect(await waitFor(() => !socketExists(root), 15_000)).toBe(true);
    socket.destroy();
  } finally {
    await cleanup(root);
  }
}, 30_000);

test("a connection that arrives as the hub starts still initializes", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  const hub = spawn(process.execPath, [main, "hub"], {
    env: environment(root),
    stdio: "ignore",
  });
  try {
    const { reply, message } = await initializeOnArrival(join(directory(root), "hub.sock"));
    expect(reply).toMatchObject({ status: "accepted" });
    expect(message).toMatchObject({ result: { protocolVersion: "2025-11-25" } });
  } finally {
    await new Promise<void>((resolve) => {
      if (hub.exitCode !== null) {
        resolve();
        return;
      }
      hub.once("exit", () => resolve());
      hub.kill("SIGKILL");
    });
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

test("the socket directory is created private", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-test-"));
  try {
    const opencode = await session(root, "opencode");
    try {
      expect(statSync(directory(root)).mode & 0o777).toBe(0o700);
    } finally {
      await opencode.close();
    }
  } finally {
    await cleanup(root);
  }
}, 30_000);
