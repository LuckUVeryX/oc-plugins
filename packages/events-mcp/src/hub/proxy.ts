import { Effect, Option, Schema } from "effect";
import { spawn } from "node:child_process";
import { closeSync, openSync } from "node:fs";
import net from "node:net";
import type { Name } from "../harness/catalog.ts";
import { isNewer, version } from "../version.ts";
import { directories, ensureDirectory, logPath, socketPath } from "./paths.ts";
import { Reply } from "./protocol.ts";

const CONNECT_TIMEOUT_MS = 60_000;
const REPLY_TIMEOUT_MS = 5_000;
// Lark allows one consumer per app, so give the old hub time to disconnect. A hub
// that stalls while exiting must not keep the client from starting a new one.
const EXIT_TIMEOUT_MS = 5_000;
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
    const child = spawn(process.execPath, [entry, "hub"], {
      detached: true,
      stdio: ["ignore", log, log],
      env: process.env,
    });
    child.unref();
  } finally {
    closeSync(log);
  }
};

const connectOrSpawn = async (entry: string, runtime: string, state: string, deadline: number) => {
  const path = socketPath(runtime);
  const existing = await connect(path);
  if (existing !== undefined) return existing;
  if (Date.now() >= deadline) return undefined;

  spawnHub(entry, state);
  while (Date.now() < deadline) {
    await sleep(RETRY_MS + Math.random() * RETRY_MS * 2);
    const socket = await connect(path);
    if (socket !== undefined) return socket;
  }
  return undefined;
};

type ReplyResult =
  | { readonly kind: "reply"; readonly reply: Reply; readonly rest: Buffer }
  | { readonly kind: "closed" }
  | { readonly kind: "timeout" }
  | { readonly kind: "invalid" };

const decodeReply = Schema.decodeUnknownOption(Schema.fromJsonString(Reply));

const readReply = (socket: net.Socket) =>
  new Promise<ReplyResult>((resolve) => {
    let buffered = Buffer.alloc(0);
    let timer: NodeJS.Timeout;
    const finish = (result: ReplyResult) => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("close", onClose);
      socket.off("error", onClose);
      socket.pause();
      resolve(result);
    };
    const onData = (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      const end = buffered.indexOf(0x0a);
      if (end === -1) return;
      const decoded = decodeReply(buffered.subarray(0, end).toString("utf8"));
      if (Option.isNone(decoded)) {
        finish({ kind: "invalid" });
        return;
      }
      finish({ kind: "reply", reply: decoded.value, rest: buffered.subarray(end + 1) });
    };
    const onClose = () => finish({ kind: "closed" });
    timer = setTimeout(() => finish({ kind: "timeout" }), REPLY_TIMEOUT_MS);
    socket.on("data", onData);
    socket.once("close", onClose);
    socket.once("error", onClose);
  });

const waitClosed = (socket: net.Socket) =>
  new Promise<void>((resolve) => {
    if (socket.destroyed) {
      resolve();
      return;
    }
    const done = () => {
      clearTimeout(timer);
      socket.off("close", done);
      socket.off("error", done);
      resolve();
    };
    const timer = setTimeout(done, EXIT_TIMEOUT_MS);
    socket.once("close", done);
    socket.once("error", done);
  });

interface BufferedStdin {
  take(): { readonly chunks: ReadonlyArray<Buffer>; readonly ended: boolean };
}

const bufferStdin = (): BufferedStdin => {
  const chunks: Buffer[] = [];
  let ended = false;
  const onData = (chunk: Buffer) => {
    chunks.push(chunk);
  };
  const onEnd = () => {
    ended = true;
  };
  process.stdin.on("data", onData);
  process.stdin.once("end", onEnd);
  process.stdin.once("close", onEnd);
  return {
    take: () => {
      process.stdin.off("data", onData);
      process.stdin.off("end", onEnd);
      process.stdin.off("close", onEnd);
      return { chunks, ended };
    },
  };
};

function exit(message: string): never {
  console.error(message);
  process.exit(1);
}

function noHub(runtime: string): never {
  exit(`events-mcp: no hub answered at ${socketPath(runtime)} within ${CONNECT_TIMEOUT_MS} ms`);
}

export const main = async (name: Name) => {
  const entry = process.argv[1];
  if (entry === undefined) exit("events-mcp: cannot find the entry script to start the hub");
  const { runtime, state } = await Effect.runPromise(directories);
  ensureDirectory(runtime);

  const input = bufferStdin();
  const deadline = Date.now() + CONNECT_TIMEOUT_MS;
  while (true) {
    const socket = await connectOrSpawn(entry, runtime, state, deadline);
    if (socket === undefined) noHub(runtime);

    socket.write(`${JSON.stringify({ harness: name, version })}\n`);
    const result = await readReply(socket);
    if (result.kind === "timeout") {
      socket.destroy();
      exit(`events-mcp: the hub did not answer within ${REPLY_TIMEOUT_MS} ms`);
    }
    if (result.kind === "invalid") {
      socket.destroy();
      exit("events-mcp: the hub sent an unexpected handshake reply");
    }
    if (result.kind === "closed") {
      socket.destroy();
      if (Date.now() >= deadline) noHub(runtime);
      await sleep(RETRY_MS + Math.random() * RETRY_MS * 2);
      continue;
    }
    if (result.reply.status === "replaced") {
      await waitClosed(socket);
      socket.destroy();
      if (Date.now() >= deadline) noHub(runtime);
      continue;
    }

    if (isNewer(version, result.reply.version))
      console.error(
        `events-mcp: proxy ${version} joined an older hub ${result.reply.version}; restart OpenCode to update the hub.`,
      );

    const buffered = input.take();
    for (const chunk of buffered.chunks) socket.write(chunk);
    if (result.rest.length > 0) process.stdout.write(result.rest);
    if (buffered.ended) socket.end();
    else process.stdin.pipe(socket);
    socket.pipe(process.stdout);
    socket.once("close", () => process.exit(0));
    socket.once("error", (error) => {
      console.error(`events-mcp: lost the hub connection: ${error.message}`);
      process.exit(1);
    });
    return;
  }
};
