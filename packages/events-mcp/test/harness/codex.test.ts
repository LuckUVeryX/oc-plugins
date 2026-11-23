import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { createServer as createNetServer, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ConfigProvider } from "effect";
import { WebSocketServer, type WebSocket } from "ws";
import { expect, test, vi } from "vitest";
import { connect } from "../support.ts";

interface Frame {
  readonly id?: number;
  readonly method?: string;
  readonly params?: Record<string, unknown>;
}

interface Add {
  readonly threadId: string;
  readonly text: string;
  readonly clientUserMessageId: string;
}

const reply = (socket: WebSocket, frame: Frame, body: Record<string, unknown>) =>
  socket.send(JSON.stringify({ jsonrpc: "2.0", id: frame.id, ...body }));

const watchParams = {
  event: "lark.im.message.receive_v1",
  id: "pipeline",
  script: "return 'wake'",
};

const fakeCodex = async (options: { failFirstAdd?: boolean } = {}) => {
  const home = await mkdtemp(join(tmpdir(), "events-mcp-codex-"));
  const control = join(home, "app-server-control");
  await mkdir(control, { recursive: true });
  const adds: Add[] = [];
  const resumes: string[] = [];
  let addCount = 0;
  const server = createHttpServer();
  const sockets = new WebSocketServer({ server });
  sockets.on("connection", (socket) => {
    socket.on("message", (data) => {
      const frame = JSON.parse(data.toString()) as Frame;
      switch (frame.method) {
        case "initialize":
          return reply(socket, frame, { result: { userAgent: "fake-codex" } });
        case "thread/read": {
          const threadId = String(frame.params?.["threadId"]);
          if (threadId !== "thread-1" && threadId !== "thread-notloaded")
            return reply(socket, frame, {
              error: { code: -32600, message: `no rollout found for thread id ${threadId}` },
            });
          return reply(socket, frame, {
            result: {
              thread: {
                id: threadId,
                status: { type: threadId === "thread-notloaded" ? "notLoaded" : "idle" },
              },
            },
          });
        }
        case "thread/queue/add":
          addCount += 1;
          if (options.failFirstAdd === true && addCount === 1)
            return reply(socket, frame, {
              error: { code: -32600, message: "no rollout found for thread id thread-1" },
            });
          adds.push({
            threadId: String(frame.params?.["threadId"]),
            text: (frame.params?.["input"] as Array<{ text: string }> | undefined)?.[0]?.text ?? "",
            clientUserMessageId: String(frame.params?.["clientUserMessageId"]),
          });
          return reply(socket, frame, { result: { queuedSubmission: {} } });
        case "thread/resume":
          resumes.push(String(frame.params?.["threadId"]));
          return reply(socket, frame, {
            result: {
              thread: { id: String(frame.params?.["threadId"]), status: { type: "idle" } },
            },
          });
        default:
          return reply(socket, frame, { error: { code: -32601, message: "unknown method" } });
      }
    });
  });
  await new Promise<void>((resolve) =>
    server.listen(join(control, "app-server-control.sock"), resolve),
  );
  return {
    home,
    adds,
    resumes,
    async close() {
      for (const client of sockets.clients) client.terminate();
      await new Promise<void>((resolve) => sockets.close(() => resolve()));
      await new Promise<void>((resolve) => server.close(() => resolve()));
      await rm(home, { recursive: true, force: true });
    },
  };
};

const withCodex = async (
  run: (codex: Awaited<ReturnType<typeof fakeCodex>>) => Promise<void>,
  options: { failFirstAdd?: boolean } = {},
) => {
  const codex = await fakeCodex(options);
  try {
    await run(codex);
  } finally {
    await codex.close();
  }
};

const codexConnect = (home: string) =>
  connect({
    harness: "codex",
    config: ConfigProvider.fromUnknown({ CODEX_HOME: home }),
  });

test("a watch wakes the given Codex thread with one queue add carrying the script result", async () => {
  await withCodex(async (codex) => {
    const h = await codexConnect(codex.home);
    try {
      const armed = await h.call("watch", {
        ...watchParams,
        threadId: "thread-1",
        script: "return `wake: ${input.event.data.message.chat_id}`",
      });
      expect(armed.id).toBe("pipeline");

      h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
      await vi.waitFor(() => expect(codex.adds).toHaveLength(1));
      expect(codex.adds[0]).toMatchObject({ threadId: "thread-1", text: "wake: oc_one" });
      expect(codex.adds[0]?.clientUserMessageId).toMatch(/^pipeline:/);
    } finally {
      await h.close();
    }
  });
});

test("a notLoaded thread gets the queued message and a resume", async () => {
  await withCodex(async (codex) => {
    const h = await codexConnect(codex.home);
    try {
      await h.call("watch", { ...watchParams, threadId: "thread-notloaded" });
      h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });
      await vi.waitFor(() => expect(codex.adds).toHaveLength(1));
      expect(codex.resumes).toEqual(["thread-notloaded"]);
    } finally {
      await h.close();
    }
  });
});

test("a watch with an unknown threadId fails before it arms", async () => {
  await withCodex(async (codex) => {
    const h = await codexConnect(codex.home);
    try {
      const result = await h.callRaw("watch", { ...watchParams, threadId: "thread-2" });
      expect(result.isError).toBe(true);
      expect(result.text).toContain("CODEX_THREAD_ID");
      expect(codex.adds).toHaveLength(0);
    } finally {
      await h.close();
    }
  });
});

test("a failed wake ends the watch with a terminated notice", async () => {
  await withCodex(
    async (codex) => {
      const h = await codexConnect(codex.home);
      try {
        await h.call("watch", { ...watchParams, threadId: "thread-1" });
        h.publish({ eventId: "one", payload: { message: { chat_id: "oc_one" } } });

        await vi.waitFor(() => expect(codex.adds).toHaveLength(1), { timeout: 5_000 });
        expect(codex.adds[0]?.text).toContain("pipeline ended: Wake failed");
        await vi.waitFor(async () => expect((await h.call("watches")).watches).toHaveLength(0));
      } finally {
        await h.close();
      }
    },
    { failFirstAdd: true },
  );
});

test("a socket that accepts but never upgrades fails the watch within the bound", async () => {
  const home = await mkdtemp(join(tmpdir(), "events-mcp-codex-"));
  const control = join(home, "app-server-control");
  await mkdir(control, { recursive: true });
  const accepted = new Set<Socket>();
  const server = createNetServer((socket) => {
    accepted.add(socket);
    socket.on("close", () => accepted.delete(socket));
  });
  await new Promise<void>((resolve) =>
    server.listen(join(control, "app-server-control.sock"), resolve),
  );
  const h = await codexConnect(home);
  const deadline = new Promise<"timeout">((resolve) =>
    setTimeout(() => resolve("timeout"), 15_000),
  );
  try {
    const result = await Promise.race([
      h.callRaw("watch", { ...watchParams, threadId: "thread-1" }),
      deadline,
    ]);
    if (result === "timeout") throw new Error("watch did not fail within 15 seconds");
    expect(result.isError).toBe(true);
    expect(result.text).toContain("10 seconds");
  } finally {
    await h.close();
    for (const socket of accepted) socket.destroy();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await rm(home, { recursive: true, force: true });
  }
});
