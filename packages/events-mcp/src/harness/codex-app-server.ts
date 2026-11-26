import { connect } from "node:net";
import { Effect, Queue, Schema, type Scope } from "effect";
import WebSocket from "ws";
import { WakeFailed } from "./harness.ts";
import { version } from "../version.ts";

const TIMEOUT = "10 seconds";

const RpcError = Schema.Struct({ code: Schema.Number, message: Schema.String });

const Frame = Schema.Struct({
  id: Schema.optionalKey(Schema.Number),
  result: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(RpcError),
});
type Frame = typeof Frame.Type;

const decodeFrame = Schema.decodeUnknownOption(Schema.fromJsonString(Frame));

type SocketEvent =
  | { readonly _tag: "Frame"; readonly frame: Frame }
  | { readonly _tag: "Closed"; readonly message: string };

interface Session {
  readonly call: (
    method: string,
    params: Record<string, unknown>,
  ) => Effect.Effect<unknown, WakeFailed>;
  readonly notify: (method: string) => Effect.Effect<void>;
}

const unreachable = (socketPath: string, message: string) =>
  `Cannot reach the Codex app-server control socket at ${socketPath}: ${message}. Start Codex so its app-server daemon runs, then try again.`;

const open = (socketPath: string): Effect.Effect<WebSocket, WakeFailed, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.callback<WebSocket, WakeFailed>((resume) => {
      const socket = new WebSocket("ws://localhost/", {
        createConnection: () => connect(socketPath),
      });
      const ready = () => resume(Effect.succeed(socket));
      const failed = (cause: Error) =>
        resume(Effect.fail(new WakeFailed({ message: unreachable(socketPath, cause.message) })));
      socket.once("open", ready);
      socket.once("error", failed);
      return Effect.sync(() => {
        socket.off("open", ready);
        socket.off("error", failed);
        // ws throws on an unhandled error when close interrupts an upgrade.
        socket.on("error", () => {});
        socket.close();
      });
    }),
    (socket) => Effect.sync(() => socket.close()),
    { interruptible: true },
  );

// withSession sends one request at a time, so a call reads frames until its own answer arrives.
const connectSession = (socketPath: string): Effect.Effect<Session, WakeFailed, Scope.Scope> =>
  Effect.gen(function* () {
    const socket = yield* open(socketPath);
    const events = yield* Queue.unbounded<SocketEvent>();
    yield* Effect.acquireRelease(
      Effect.sync(() => {
        const onMessage = (data: unknown) => {
          const decoded = decodeFrame(String(data));
          if (decoded._tag === "Some")
            Queue.offerUnsafe(events, { _tag: "Frame", frame: decoded.value });
        };
        const onClose = () =>
          Queue.offerUnsafe(events, { _tag: "Closed", message: "the socket closed" });
        const onError = (cause: Error) =>
          Queue.offerUnsafe(events, { _tag: "Closed", message: cause.message });
        socket.on("message", onMessage);
        socket.on("close", onClose);
        socket.on("error", onError);
        return () => {
          socket.off("message", onMessage);
          socket.off("close", onClose);
          socket.off("error", onError);
        };
      }),
      (remove) => Effect.sync(remove),
    );

    let lastId = 0;
    const answer = (id: number): Effect.Effect<unknown, WakeFailed> =>
      Queue.take(events).pipe(
        Effect.flatMap((event) => {
          if (event._tag === "Closed")
            return Effect.fail(new WakeFailed({ message: event.message }));
          const { frame } = event;
          if (frame.id !== id) return answer(id);
          if (frame.error !== undefined)
            return Effect.fail(
              new WakeFailed({ message: `Codex rejected the request: ${frame.error.message}` }),
            );
          return Effect.succeed(frame.result);
        }),
      );

    return {
      call: (method, params) =>
        Effect.suspend(() => {
          lastId += 1;
          const id = lastId;
          socket.send(JSON.stringify({ jsonrpc: "2.0", id, method, params }));
          return answer(id);
        }),
      notify: (method) =>
        Effect.sync(() => socket.send(JSON.stringify({ jsonrpc: "2.0", method }))),
    };
  });

const withSession = <A>(
  socketPath: string,
  body: (session: Session) => Effect.Effect<A, WakeFailed>,
): Effect.Effect<A, WakeFailed> =>
  Effect.gen(function* () {
    const session = yield* connectSession(socketPath);
    yield* session.call("initialize", {
      clientInfo: { name: "events-mcp", title: "events-mcp", version },
      capabilities: { experimentalApi: true },
    });
    yield* session.notify("initialized");
    return yield* body(session);
  }).pipe(
    Effect.scoped,
    Effect.timeout(TIMEOUT),
    Effect.catchTag("TimeoutError", () =>
      Effect.fail(
        new WakeFailed({ message: unreachable(socketPath, `no answer within ${TIMEOUT}`) }),
      ),
    ),
  );

const ThreadRead = Schema.Struct({
  thread: Schema.Struct({
    id: Schema.String,
    status: Schema.Struct({ type: Schema.String }),
  }),
});
type ThreadRead = typeof ThreadRead.Type;

const readThread = (session: Session, threadId: string): Effect.Effect<ThreadRead, WakeFailed> =>
  Effect.gen(function* () {
    const result = yield* session.call("thread/read", { threadId });
    const decoded = Schema.decodeUnknownOption(ThreadRead)(result);
    if (decoded._tag === "None")
      return yield* Effect.fail(
        new WakeFailed({ message: "Codex returned an unexpected thread/read response" }),
      );
    if (decoded.value.thread.id !== threadId)
      return yield* Effect.fail(
        new WakeFailed({ message: `Codex answered for thread ${decoded.value.thread.id}` }),
      );
    return decoded.value;
  });

const unknownThread = (threadId: string) =>
  `Codex does not know thread "${threadId}". Run printf %s "$CODEX_THREAD_ID" in this thread's shell and pass that exact value.`;

/** Fails unless Codex knows this thread. Run at watch time, before the watch arms. */
export const checkThread = (
  socketPath: string,
  threadId: string,
): Effect.Effect<void, WakeFailed> =>
  withSession(socketPath, (session) => readThread(session, threadId).pipe(Effect.asVoid)).pipe(
    Effect.catchTag("Harness.WakeFailed", (error) =>
      Effect.fail(
        new WakeFailed({
          message: /no rollout found|thread not found/i.test(error.message)
            ? unknownThread(threadId)
            : error.message,
        }),
      ),
    ),
  );

/**
 * Adds `text` to the thread's queue as a user message, which starts a turn when the thread is idle.
 * A notLoaded thread stores the message and needs a resume to dispatch it.
 */
export const queue = (
  socketPath: string,
  input: {
    readonly threadId: string;
    readonly text: string;
    readonly clientUserMessageId: string;
  },
): Effect.Effect<void, WakeFailed> =>
  withSession(socketPath, (session) =>
    Effect.gen(function* () {
      const { thread } = yield* readThread(session, input.threadId);
      yield* session.call("thread/queue/add", {
        threadId: input.threadId,
        input: [{ type: "text", text: input.text }],
        clientUserMessageId: input.clientUserMessageId,
      });
      if (thread.status.type === "notLoaded")
        yield* session.call("thread/resume", { threadId: input.threadId });
    }),
  );

export * as AppServer from "./codex-app-server.ts";
