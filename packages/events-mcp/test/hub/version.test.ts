import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";
import { Schema } from "effect";
import { spawn, type ChildProcess } from "node:child_process";
import {
  closeSync,
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const packageRoot = fileURLToPath(new URL("../..", import.meta.url));
const nodeModules = fileURLToPath(new URL("../../../../node_modules", import.meta.url));

const Tools = Schema.toStandardSchemaV1(
  Schema.Struct({ tools: Schema.Array(Schema.Struct({ name: Schema.String })) }),
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

const waitFor = async (check: () => boolean, timeoutMs = 10_000) => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (check()) return true;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  return check();
};

const socketExists = (root: string) => existsSync(join(root, "run", "hub.sock"));

const listening = (root: string) => {
  const log = readFileSync(join(root, "state", "hub.log"), "utf8");
  return log.split("\n").filter((line) => line.includes(": listening at ")).length;
};

const copyPackage = (root: string, version: string) => {
  const directory = join(root, `events-mcp-${version}`);
  cpSync(join(packageRoot, "src"), join(directory, "src"), { recursive: true });
  writeFileSync(join(directory, "package.json"), JSON.stringify({ version }));
  symlinkSync(nodeModules, join(directory, "node_modules"), "dir");
  return join(directory, "src", "main.ts");
};

const waitExit = (child: ChildProcess) =>
  new Promise<number | null>((resolve) => {
    if (child.exitCode !== null) {
      resolve(child.exitCode);
      return;
    }
    child.once("exit", (code) => resolve(code));
  });

const stop = async (child: ChildProcess) => {
  if (child.exitCode !== null) return;
  const exited = waitExit(child);
  child.kill("SIGKILL");
  await exited;
};

const startHub = async (main: string, root: string) => {
  mkdirSync(join(root, "run"), { recursive: true });
  mkdirSync(join(root, "state"), { recursive: true });
  const log = openSync(join(root, "state", "hub.log"), "a", 0o600);
  let child: ChildProcess;
  try {
    child = spawn(process.execPath, [main, "hub"], {
      env: environment(root),
      stdio: ["ignore", log, log],
    });
  } finally {
    closeSync(log);
  }
  if (!(await waitFor(() => socketExists(root))))
    throw new Error("the hub did not create its socket");
  return child;
};

const session = async (
  main: string,
  root: string,
  harness: string,
  stderr: "ignore" | "pipe" = "ignore",
) => {
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [main, harness],
    env: environment(root),
    stderr,
  });
  let diagnostics = "";
  transport.stderr?.on("data", (chunk) => {
    diagnostics += String(chunk);
  });
  const client = new Client({ name: "hub-test", version: "0" });
  await client.connect(transport);
  return { client, transport, diagnostics: () => diagnostics };
};

const toolNames = async (client: Client) =>
  (await client.request({ method: "tools/list", params: {} }, Tools)).tools.map(
    (tool) => tool.name,
  );

test("a newer proxy replaces an older hub when no OpenCode session is connected", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-version-"));
  const clients: Client[] = [];
  const children: ChildProcess[] = [];
  try {
    const oldMain = copyPackage(root, "0.0.0");
    const newMain = copyPackage(root, "9.9.9");
    const oldHub = await startHub(oldMain, root);
    children.push(oldHub);
    const old = await session(oldMain, root, "claude");
    clients.push(old.client);
    expect(await toolNames(old.client)).toContain("watch");

    const oldExited = waitExit(oldHub);
    const newer = await session(newMain, root, "claude");
    clients.push(newer.client);
    await oldExited;
    expect(await toolNames(newer.client)).toContain("watch");
    expect(listening(root)).toBe(2);
  } finally {
    for (const client of clients) await client.close().catch(() => {});
    for (const child of children) await stop(child);
    await waitFor(() => !socketExists(root));
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

test("a newer proxy joins a hub while an OpenCode session is connected", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-version-"));
  const clients: Client[] = [];
  try {
    const oldMain = copyPackage(root, "0.0.0");
    const newMain = copyPackage(root, "9.9.9");
    await startHub(oldMain, root);
    const opencode = await session(oldMain, root, "opencode");
    clients.push(opencode.client);
    expect(await toolNames(opencode.client)).not.toContain("watch");

    const newer = await session(newMain, root, "claude", "pipe");
    clients.push(newer.client);
    expect(await toolNames(newer.client)).toContain("watch");
    expect(await toolNames(opencode.client)).not.toContain("watch");
    expect(
      await waitFor(() => newer.diagnostics().includes("restart OpenCode to update the hub")),
    ).toBe(true);
    expect(newer.diagnostics()).toContain("0.0.0");
    expect(newer.diagnostics()).toContain("9.9.9");
    expect(listening(root)).toBe(1);
  } finally {
    for (const client of clients) await client.close().catch(() => {});
    await waitFor(() => !socketExists(root));
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

test("an older proxy works with a newer hub", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-version-"));
  const clients: Client[] = [];
  try {
    const newMain = copyPackage(root, "9.9.9");
    const oldMain = copyPackage(root, "0.0.0");
    await startHub(newMain, root);
    const older = await session(oldMain, root, "claude");
    clients.push(older.client);
    expect(await toolNames(older.client)).toContain("watch");
    expect(listening(root)).toBe(1);
  } finally {
    for (const client of clients) await client.close().catch(() => {});
    await waitFor(() => !socketExists(root));
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

test("two newer proxies started together end up on one hub", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-version-"));
  const clients: Client[] = [];
  try {
    const oldMain = copyPackage(root, "0.0.0");
    const newMain = copyPackage(root, "9.9.9");
    await startHub(oldMain, root);
    const sessions = await Promise.all([
      session(newMain, root, "claude"),
      session(newMain, root, "claude"),
    ]);
    for (const { client } of sessions) clients.push(client);
    for (const { client } of sessions) expect(await toolNames(client)).toContain("watch");
    expect(listening(root)).toBe(2);
  } finally {
    for (const client of clients) await client.close().catch(() => {});
    await waitFor(() => !socketExists(root), 15_000);
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);

test("a hub that never replies reports the version check it predates", async () => {
  const root = mkdtempSync(join(tmpdir(), "events-hub-version-"));
  mkdirSync(join(root, "run"), { recursive: true });
  const sockets: net.Socket[] = [];
  const server = net.createServer((socket) => sockets.push(socket));
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(join(root, "run", "hub.sock"), () => resolve());
  });
  try {
    const newMain = copyPackage(root, "9.9.9");
    const child = spawn(process.execPath, [newMain, "claude"], {
      env: environment(root),
      stdio: ["pipe", "ignore", "pipe"],
    });
    let diagnostics = "";
    child.stderr.on("data", (chunk) => {
      diagnostics += String(chunk);
    });
    const code = await Promise.race([
      waitExit(child),
      new Promise<"timeout">((resolve) => setTimeout(() => resolve("timeout"), 12_000)),
    ]);
    if (code === "timeout") await stop(child);
    expect(code).toBe(1);
    expect(diagnostics).toContain("predates version checks");
    expect(diagnostics).toContain("OpenCode, Claude Code and Codex");
  } finally {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    rmSync(root, { recursive: true, force: true });
  }
}, 30_000);
