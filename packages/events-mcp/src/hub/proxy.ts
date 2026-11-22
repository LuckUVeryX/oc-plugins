import { Effect } from "effect";
import { spawn } from "node:child_process";
import { closeSync, openSync } from "node:fs";
import net from "node:net";
import type { Name } from "../harness/index.ts";
import { directories, ensureDirectory, logPath, socketPath } from "./paths.ts";

const CONNECT_TIMEOUT_MS = 60_000;
const RETRY_MS = 25;

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

const connect = (path: string) =>
  new Promise<net.Socket | undefined>((resolve) => {
    const socket = net.connect({ path });
    const fail = () => {
      socket.destroy();
      resolve(undefined);
    };
    socket.once("connect", () => {
      socket.off("error", fail);
      resolve(socket);
    });
    socket.once("error", fail);
  });

const spawnHub = (entry: string, state: string) => {
  ensureDirectory(state);
  const log = openSync(logPath(state), "a", 0o600);
  try {
    const child = spawn(process.execPath, [entry, "hub", String(process.pid)], {
      detached: true,
      stdio: ["ignore", log, log],
      env: process.env,
    });
    child.unref();
  } finally {
    closeSync(log);
  }
};

const connectOrSpawn = async (entry: string, runtime: string, state: string) => {
  const path = socketPath(runtime);
  const existing = await connect(path);
  if (existing !== undefined) return existing;

  spawnHub(entry, state);
  const deadline = Date.now() + CONNECT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await sleep(RETRY_MS + Math.random() * RETRY_MS * 2);
    const socket = await connect(path);
    if (socket !== undefined) return socket;
  }
  return undefined;
};

export const main = async (name: Name) => {
  const entry = process.argv[1];
  if (entry === undefined) {
    console.error("events-mcp: cannot find the entry script to start the hub");
    process.exit(1);
  }
  const { runtime, state } = await Effect.runPromise(directories);
  ensureDirectory(runtime);

  const socket = await connectOrSpawn(entry, runtime, state);
  if (socket === undefined) {
    console.error(
      `events-mcp: no hub answered at ${socketPath(runtime)} within ${CONNECT_TIMEOUT_MS} ms`,
    );
    process.exit(1);
  }

  socket.write(`${JSON.stringify({ harness: name })}\n`);
  process.stdin.pipe(socket);
  socket.pipe(process.stdout);
  socket.once("close", () => process.exit(0));
  socket.once("error", (error) => {
    console.error(`events-mcp: lost the hub connection: ${error.message}`);
    process.exit(1);
  });
};
