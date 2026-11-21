import { expect, test } from "vitest";
import { Effect, Fiber, Option, Queue, Schedule, Schema, Stream } from "effect";
import {
  GitlabSocket,
  type ChannelIdentifier,
  type Json,
  makeGitlabSocket,
} from "../../src/gitlab/socket.ts";
import { Health, type IntegrationError } from "../../src/core/integration.ts";

const WireFrame = Schema.Record(Schema.String, Schema.Unknown);

type Frame = typeof WireFrame.Type;

const decodeFrame = Schema.decodeUnknownSync(WireFrame);

const frameIdentifier = (frame: Frame | undefined): string => {
  if (frame === undefined) throw new Error("missing frame");
  if (typeof frame.identifier !== "string") throw new Error("missing identifier");
  return frame.identifier;
};

const startServer = (
  options: {
    readonly respond?: "confirm" | "reject";
    readonly welcome?: "welcome" | "disconnect";
    readonly ping?: number;
    readonly pingFrom?: number;
  } = {},
) => {
  const frames: Array<Frame> = [];
  const sockets = new Set<Bun.ServerWebSocket<unknown>>();
  const timers = new Map<Bun.ServerWebSocket<unknown>, ReturnType<typeof setInterval>>();
  const waiters = new Set<() => void>();
  let connections = 0;
  const notify = () => {
    for (const waiter of waiters) waiter();
  };
  const waitFor = (predicate: () => boolean) => {
    if (predicate()) return Promise.resolve();
    return new Promise<void>((resolve) => {
      const waiter = () => {
        if (!predicate()) return;
        waiters.delete(waiter);
        resolve();
      };
      waiters.add(waiter);
    });
  };
  const server = Bun.serve({
    port: 0,
    fetch(request, server) {
      if (server.upgrade(request)) return undefined;
      return new Response("upgrade required", { status: 400 });
    },
    websocket: {
      open(socket) {
        connections += 1;
        sockets.add(socket);
        if (options.welcome === "disconnect")
          socket.send(
            JSON.stringify({ type: "disconnect", reconnect: false, reason: "unauthorized" }),
          );
        else socket.send(JSON.stringify({ type: "welcome" }));
        if (options.ping !== undefined && connections >= (options.pingFrom ?? 1)) {
          timers.set(
            socket,
            setInterval(
              () => socket.send(JSON.stringify({ type: "ping", message: {} })),
              options.ping,
            ),
          );
        }
        notify();
      },
      message(socket, data) {
        const parsed: unknown = JSON.parse(String(data));
        const frame = decodeFrame(parsed);
        frames.push(frame);
        if (frame.command === "subscribe")
          socket.send(
            JSON.stringify({
              identifier: frame.identifier,
              type: options.respond === "reject" ? "reject_subscription" : "confirm_subscription",
            }),
          );
        notify();
      },
      close(socket) {
        sockets.delete(socket);
        const timer = timers.get(socket);
        if (timer !== undefined) clearInterval(timer);
        timers.delete(socket);
        notify();
      },
    },
  });
  const broadcast = (frame: Frame) => {
    for (const socket of sockets) socket.send(JSON.stringify(frame));
  };
  return {
    url: `ws://localhost:${server.port}/`,
    waitFor,
    connections: () => connections,
    subscribes: () => frames.filter((frame) => frame.command === "subscribe"),
    unsubscribes: () => frames.filter((frame) => frame.command === "unsubscribe"),
    message: (identifier: string, message: Json) => broadcast({ identifier, message }),
    disconnect: (reconnect: boolean, reason: string) =>
      broadcast({ type: "disconnect", reconnect, reason }),
    drop: () => {
      for (const socket of sockets) socket.terminate();
    },
    stop: () => {
      for (const timer of timers.values()) clearInterval(timer);
      server.stop(true);
    },
  };
};

const startCable = async (
  url: string,
  options: { readonly pingTimeout?: number; readonly backoff?: Schedule.Schedule<unknown> } = {},
) => {
  const ready = Promise.withResolvers<GitlabSocket.Interface>();
  const fiber = Effect.runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const socket = yield* makeGitlabSocket({
          url,
          headers: Effect.succeed({}),
          pingTimeout: options.pingTimeout,
          backoff: options.backoff ?? Schedule.spaced("10 millis"),
        });
        ready.resolve(socket);
        yield* Effect.never;
      }),
    ),
  );
  return { socket: await ready.promise, stop: () => Effect.runPromise(Fiber.interrupt(fiber)) };
};

const hold = async (socket: GitlabSocket.Interface, identifier: ChannelIdentifier) => {
  const messages = await Effect.runPromise(Queue.unbounded<Json>());
  const fiber = Effect.runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const stream = yield* socket.subscribe(identifier);
        yield* stream.pipe(
          Stream.runForEach((message) => Queue.offer(messages, message)),
          Effect.forkScoped,
        );
        yield* Effect.never;
      }),
    ),
  );
  return { messages, release: () => Effect.runPromise(Fiber.interrupt(fiber)) };
};

const healthLog = (socket: GitlabSocket.Interface) => {
  const values = Effect.runSync(Queue.unbounded<Health>());
  const tags: Array<string> = [];
  const fiber = Effect.runFork(
    socket.health.pipe(
      Stream.runForEach((value) =>
        Effect.sync(() => tags.push(value._tag)).pipe(Effect.andThen(Queue.offer(values, value))),
      ),
    ),
  );
  return {
    tags,
    next: async (predicate: (value: Health) => boolean) => {
      while (true) {
        const value = await Effect.runPromise(Queue.take(values));
        if (predicate(value)) return value;
      }
    },
    stop: () => Effect.runPromise(Fiber.interrupt(fiber)),
  };
};

const receive = (messages: Queue.Queue<Json>) =>
  Effect.runPromise(Queue.take(messages).pipe(Effect.timeoutOption("2 seconds")));

const identifier = (nonce: string) => ({
  channel: "GraphqlChannel",
  query: "subscription { ciPipelineStatusesUpdated { id } }",
  variables: { projectFullPath: "group/example" },
  operationName: null,
  nonce,
});

test("fails the feed when the server rejects it", async () => {
  const server = startServer({ respond: "reject" });
  const { socket, stop } = await startCable(server.url);
  try {
    const failure = await Effect.runPromise(
      Effect.scoped(socket.subscribe(identifier("rejected"))).pipe(Effect.flip),
    );
    expect(failure._tag).toBe("Integration.Rejected");
  } finally {
    await stop();
    server.stop();
  }
});

test("resubscribes and returns to Live after a dropped connection", async () => {
  const server = startServer();
  const { socket, stop } = await startCable(server.url);
  const held = await hold(socket, identifier("drop"));
  const log = healthLog(socket);
  try {
    await server.waitFor(() => server.subscribes().length === 1);
    await log.next((value) => value._tag === "Live");
    server.drop();
    await server.waitFor(() => server.connections() === 2 && server.subscribes().length === 2);
    await log.next((value) => value._tag === "Live");
    const wire = frameIdentifier(server.subscribes()[1]);
    server.message(wire, { event: "after-reconnect" });
    const received = await receive(held.messages);
    expect(Option.isSome(received) && received.value).toEqual({ event: "after-reconnect" });
    expect(log.tags).toContain("Degraded");
    expect(log.tags.at(-1)).toBe("Live");
  } finally {
    await log.stop();
    await held.release();
    await stop();
    server.stop();
  }
});

test("fails every feed when the server refuses to reconnect", async () => {
  const server = startServer();
  const { socket, stop } = await startCable(server.url);
  const failed = Promise.withResolvers<IntegrationError>();
  const fiber = Effect.runFork(
    Effect.scoped(
      Effect.gen(function* () {
        const stream = yield* socket.subscribe(identifier("unauthorized"));
        yield* stream.pipe(
          Stream.runDrain,
          Effect.catchTag("Integration.Rejected", (error) =>
            Effect.sync(() => failed.resolve(error)),
          ),
        );
      }),
    ),
  );
  try {
    await server.waitFor(() => server.subscribes().length === 1);
    server.disconnect(false, "unauthorized");
    expect((await failed.promise)._tag).toBe("Integration.Rejected");
  } finally {
    await Effect.runPromise(Fiber.interrupt(fiber));
    await stop();
    server.stop();
  }
});

test("reconnects after missed pings", async () => {
  const server = startServer({ ping: 5, pingFrom: 2 });
  const { socket, stop } = await startCable(server.url, { pingTimeout: 25 });
  const held = await hold(socket, identifier("ping"));
  try {
    await server.waitFor(() => server.connections() >= 2 && server.subscribes().length >= 2);
    await Effect.runPromise(
      socket.health.pipe(
        Stream.filter((value) => value._tag === "Live"),
        Stream.take(1),
        Stream.runDrain,
      ),
    );
    const wire = frameIdentifier(server.subscribes().at(-1));
    server.message(wire, { event: "after-ping-timeout" });
    const received = await receive(held.messages);
    expect(Option.isSome(received) && received.value).toEqual({ event: "after-ping-timeout" });
  } finally {
    await held.release();
    await stop();
    server.stop();
  }
});

test("fails the feed when the server refuses the token before welcoming", async () => {
  const server = startServer({ welcome: "disconnect" });
  const { socket, stop } = await startCable(server.url);
  try {
    const failure = await Effect.runPromise(
      Effect.scoped(socket.subscribe(identifier("unauthorized-before-welcome"))).pipe(Effect.flip),
    );
    expect(failure._tag).toBe("Integration.Rejected");
    expect(server.connections()).toBe(1);
  } finally {
    await stop();
    server.stop();
  }
});
