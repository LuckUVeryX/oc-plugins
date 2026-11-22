import { BunRuntime } from "@effect/platform-bun";
import { StdioServerTransport, serveStdio } from "@modelcontextprotocol/server/stdio";
import { Flock } from "@opencode/util/flock";
import {
  Cause,
  ConfigProvider,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Logger,
  Option,
  Queue,
  Runtime,
  Schema,
  Scope,
} from "effect";
import { FetchHttpClient } from "effect/http";
import { rmSync } from "node:fs";
import net from "node:net";
import { PassThrough } from "node:stream";
import { DecisionModel } from "../core/decision-model.ts";
import { Feeds } from "../core/feeds.ts";
import { Registry } from "../core/registry.ts";
import { Server } from "../core/server.ts";
import { GitlabAuth } from "../gitlab/auth.ts";
import { GitlabClient } from "../gitlab/client.ts";
import { Gitlab } from "../gitlab/integration.ts";
import { GitlabSocket } from "../gitlab/socket.ts";
import { isName, type Name } from "../harness/index.ts";
import { LarkAuth } from "../lark/auth.ts";
import { Lark } from "../lark/integration.ts";
import { LarkSocket } from "../lark/socket.ts";
import { directories, ensureDirectory, lockPath, socketPath } from "./paths.ts";

const HELLO_TIMEOUT = "5 seconds";
const MAX_HELLO_BYTES = 512;
const SPAWNER_TICK = "1 second";
const RETIRE_AFTER = "1 second";

const Hello = Schema.Struct({ harness: Schema.String });

const isLive = (path: string) =>
  new Promise<boolean>((resolve) => {
    const probe = net.connect({ path });
    const done = (live: boolean) => {
      probe.destroy();
      resolve(live);
    };
    probe.once("connect", () => done(true));
    probe.once("error", () => done(false));
    probe.setTimeout(1_000, () => done(false));
  });

const listen = (entry: net.Server, path: string) =>
  new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => reject(error);
    entry.once("error", onError);
    entry.listen(path, () => {
      entry.off("error", onError);
      resolve();
    });
  });

const isAlive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return !(
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      error.code === "ESRCH"
    );
  }
};

type HelloResult = { readonly name: Name; readonly rest: Buffer } | { readonly reason: string };

const parseHello = (line: Buffer): { readonly name: Name } | { readonly reason: string } => {
  let value: unknown;
  try {
    value = JSON.parse(line.toString("utf8"));
  } catch {
    return { reason: "the hello is not JSON" };
  }
  const decoded = Schema.decodeUnknownOption(Hello)(value);
  if (Option.isNone(decoded)) return { reason: "the hello is missing harness" };
  if (!isName(decoded.value.harness))
    return { reason: `unknown harness "${decoded.value.harness}"` };
  return { name: decoded.value.harness };
};

const readHello = (socket: net.Socket) =>
  Effect.callback<HelloResult>((resume) => {
    let buffered = Buffer.alloc(0);
    const detach = () => {
      socket.off("data", onData);
      socket.off("close", onClose);
      socket.pause();
    };
    const finish = (line?: Buffer, rest?: Buffer) => {
      detach();
      if (line === undefined || rest === undefined) {
        resume(Effect.succeed({ reason: "the connection closed before its hello" }));
        return;
      }
      const parsed = parseHello(line);
      resume(Effect.succeed("reason" in parsed ? parsed : { name: parsed.name, rest }));
    };
    const onData = (chunk: Buffer) => {
      buffered = Buffer.concat([buffered, chunk]);
      const end = buffered.indexOf(0x0a);
      if (end === -1) {
        if (buffered.length > MAX_HELLO_BYTES) finish(undefined, undefined);
        return;
      }
      finish(buffered.subarray(0, end), buffered.subarray(end + 1));
    };
    const onClose = () => finish();
    socket.on("data", onData);
    socket.on("close", onClose);
    return Effect.sync(detach);
  }).pipe(
    Effect.timeoutOption(HELLO_TIMEOUT),
    Effect.map(Option.getOrElse(() => ({ reason: `no hello within ${HELLO_TIMEOUT}` }))),
  );

const bind = (entry: net.Server, runtime: string) =>
  Effect.gen(function* () {
    const path = socketPath(runtime);
    const owned = yield* Effect.promise(() =>
      Flock.withLock(
        "events-hub",
        async () => {
          if (await isLive(path)) return false;
          rmSync(path, { force: true });
          await listen(entry, path);
          return true;
        },
        {
          dir: lockPath(runtime),
          staleMs: 1_000,
          timeoutMs: 5_000,
          baseDelayMs: 10,
          maxDelayMs: 50,
        },
      ),
    );
    yield* Effect.logInfo(
      owned ? `listening at ${path} as pid ${process.pid}` : `another hub is listening at ${path}`,
    );
    return owned;
  });

export interface Interface {
  readonly done: Effect.Effect<void>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/Hub") {}

type Dependencies = Registry.Service | Feeds.Service | DecisionModel.Service;

interface SessionState {
  active: number;
  served: boolean;
}

interface Session {
  readonly entry: net.Server;
  readonly queue: Queue.Queue<net.Socket>;
  readonly state: SessionState;
  readonly spawner: number;
  readonly scope: Scope.Scope;
  readonly idle: Deferred.Deferred<void>;
}

// Exit only when no connection is open or queued and either a session has been
// served or the proxy that spawned this hub is gone.
const idleNow = (session: Session) =>
  session.state.active === 0 &&
  Queue.sizeUnsafe(session.queue) === 0 &&
  (session.state.served || !isAlive(session.spawner));

// The read and the close run in one synchronous step so an arriving connection
// cannot slip between them. A short settle first gives a harness that disposed
// its negotiation probe time to open its real connection.
const retire = (session: Session) =>
  Effect.gen(function* () {
    yield* Effect.sleep(RETIRE_AFTER);
    const retired = yield* Effect.sync(() => {
      if (!idleNow(session)) return false;
      session.entry.close();
      return true;
    });
    if (!retired) return;
    yield* Effect.logInfo("no sessions, exiting");
    yield* Deferred.succeed(session.idle, undefined);
  });

const evaluate = (session: Session) =>
  Effect.gen(function* () {
    if (!(yield* Effect.sync(() => idleNow(session)))) return;
    yield* Effect.forkIn(session.scope)(retire(session));
  });

const watchSpawner = (session: Session) =>
  Effect.gen(function* () {
    while (!session.state.served) {
      yield* Effect.sleep(SPAWNER_TICK);
      if (session.state.served) return;
      yield* evaluate(session);
    }
  });

const connection = (socket: net.Socket, session: Session) =>
  Effect.scopedWith((scope) =>
    Effect.gen(function* () {
      const hello = yield* readHello(socket);
      if ("reason" in hello) {
        yield* Effect.logWarning(`closed before its harness arrived: ${hello.reason}`);
        return;
      }
      session.state.served = true;
      yield* Effect.logInfo(`session opened: ${hello.name}`);

      const input = new PassThrough();
      input.write(hello.rest);
      socket.pipe(input);
      socket.once("close", () => input.end());

      const transport = new StdioServerTransport(input, socket);
      const closed = yield* Deferred.make<void>();
      const server = yield* Server.make({ harness: hello.name, scope });
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          const handle = serveStdio(() => server, {
            legacy: "serve",
            transport,
            onerror: (error) => Effect.runSync(Effect.logError(error)),
          });
          const onclose = transport.onclose;
          transport.onclose = () => {
            onclose?.();
            Deferred.doneUnsafe(closed, Effect.void);
          };
          return handle;
        }),
        (handle) => Effect.promise(() => handle.close()),
      );
      yield* Deferred.await(closed);
    }).pipe(
      Effect.ensuring(Effect.sync(() => socket.destroy())),
      Effect.onExit(() => Effect.logInfo("session closed")),
    ),
  );

const serve = (session: Session) =>
  Effect.gen(function* () {
    while (true) {
      const socket = yield* Queue.take(session.queue);
      yield* Effect.forkScoped(
        connection(socket, session).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              session.state.active -= 1;
              yield* evaluate(session);
            }),
          ),
        ),
      );
    }
  });

export const layer: Layer.Layer<Service, never, Dependencies> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const spawner = yield* Effect.sync(() => {
      const value = Number(process.argv[3]);
      if (!Number.isInteger(value) || value <= 0)
        throw new Error("events-mcp hub is started by a proxy, not directly");
      return value;
    });
    const paths = yield* directories;
    yield* Effect.sync(() => ensureDirectory(paths.runtime));

    const queue = yield* Queue.unbounded<net.Socket>();
    const state: SessionState = { active: 0, served: false };
    const idle = yield* Deferred.make<void>();
    const entry = net.createServer();

    // Attach the handler before listen. A connection accepted between listen and
    // the first handler waits in the queue instead of being dropped.
    entry.on("connection", (socket) => {
      state.active += 1;
      Queue.offerUnsafe(queue, socket);
    });

    if (!(yield* bind(entry, paths.runtime))) return Service.of({ done: Effect.void });

    // close() unlinks the socket. A replacement hub can bind the path after our
    // close, so nothing may remove it again.
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        if (entry.listening) entry.close();
      }),
    );

    const session: Session = {
      entry,
      queue,
      state,
      spawner,
      scope: yield* Effect.scope,
      idle,
    };
    yield* Effect.forkScoped(serve(session));
    yield* Effect.forkIn(session.scope)(watchSpawner(session));
    return Service.of({ done: Deferred.await(session.idle) });
  }),
);

const LarkLayers = LarkSocket.layer.pipe(Layer.provideMerge(LarkAuth.layer));
const GitlabLayers = Layer.merge(GitlabSocket.layer, GitlabClient.layer).pipe(
  Layer.provideMerge(GitlabAuth.layer),
);
const IntegrationLayers = Layer.merge(LarkLayers, GitlabLayers).pipe(
  Layer.provide(FetchHttpClient.layer),
);
const RegistryLayer = Registry.layer(Effect.all([Lark.integration, Gitlab.integration])).pipe(
  Layer.provide(IntegrationLayers),
);
const CoreLayer = Layer.mergeAll(Feeds.layer, DecisionModel.layer, RegistryLayer);

export const main = () => {
  const program = Effect.gen(function* () {
    const hub = yield* Service;
    yield* hub.done;
  }).pipe(
    Effect.provide(layer.pipe(Layer.provide(CoreLayer))),
    Effect.provide(ConfigProvider.layer(ConfigProvider.fromEnv({ preserveEmptyStrings: true }))),
    Effect.provideService(Logger.LogToStderr, true),
    Effect.scoped,
  );

  BunRuntime.runMain(program, {
    teardown: (exit, onExit) =>
      Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)
        ? onExit(0)
        : Runtime.defaultTeardown(exit, onExit),
  });
};
