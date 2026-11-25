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
import { CodemagicClient } from "../codemagic/client.ts";
import { Codemagic } from "../codemagic/source.ts";
import { CodemagicSocket } from "../codemagic/socket.ts";
import { DecisionModel } from "../decision/decision-model.ts";
import { Feeds } from "../watch/feeds.ts";
import { Registry } from "../source/registry.ts";
import { Server } from "../server/server.ts";
import type { Watch } from "../server/tools.ts";
import { GitlabAuth } from "../gitlab/auth.ts";
import { GitlabClient } from "../gitlab/client.ts";
import { Gitlab } from "../gitlab/source.ts";
import { GitlabSocket } from "../gitlab/socket.ts";
import { isName, type Name } from "../harness/catalog.ts";
import { LarkAuth } from "../lark/auth.ts";
import { Lark } from "../lark/source.ts";
import { LarkSocket } from "../lark/socket.ts";
import { version } from "../version.ts";
import { directories, ensureDirectory, lockPath, socketPath } from "./paths.ts";
import { encodeReply, Hello, type Reply } from "./protocol.ts";

const HELLO_TIMEOUT = "5 seconds";
const MAX_HELLO_BYTES = 512;
const RETIRE_AFTER = "1 second";
// A proxy that spawns the hub connects within its own 60-second connect timeout.
// A hub nobody connects to by then exits, for example when its proxy died first.
const STARTUP_GRACE = "60 seconds";

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

type HelloResult =
  | { readonly name: Name; readonly version: string; readonly rest: Buffer }
  | { readonly reason: string };

const parseHello = (
  line: Buffer,
): { readonly name: Name; readonly version: string } | { readonly reason: string } => {
  let value: unknown;
  try {
    value = JSON.parse(line.toString("utf8"));
  } catch {
    return { reason: "the hello is not JSON" };
  }
  const decoded = Schema.decodeUnknownOption(Hello)(value);
  if (Option.isNone(decoded)) return { reason: "the hello is missing harness or version" };
  if (!isName(decoded.value.harness))
    return { reason: `unknown harness "${decoded.value.harness}"` };
  return { name: decoded.value.harness, version: decoded.value.version };
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
      resume(Effect.succeed("reason" in parsed ? parsed : { ...parsed, rest }));
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

interface HubState {
  connections: number;
  opencode: number;
  retiring: boolean;
}

interface Hub {
  readonly entry: net.Server;
  readonly queue: Queue.Queue<net.Socket>;
  readonly state: HubState;
  readonly sockets: Set<net.Socket>;
  readonly scope: Scope.Scope;
  readonly idle: Deferred.Deferred<void>;
  readonly watches: Map<string, Watch>;
}

const idleNow = (hub: Hub) => hub.state.connections === 0 && Queue.sizeUnsafe(hub.queue) === 0;

// The read and the close run in one synchronous step so an arriving connection
// cannot slip between them. A short settle first gives a harness that disposed
// its negotiation probe time to open its real connection.
const retire = (hub: Hub) =>
  Effect.gen(function* () {
    yield* Effect.sleep(RETIRE_AFTER);
    const retired = yield* Effect.sync(() => {
      if (!idleNow(hub)) return false;
      hub.state.retiring = true;
      hub.entry.close();
      return true;
    });
    if (!retired) return;
    yield* Effect.logInfo("no connections, exiting");
    yield* Deferred.succeed(hub.idle, undefined);
  });

const evaluate = (hub: Hub) =>
  Effect.gen(function* () {
    if (!(yield* Effect.sync(() => idleNow(hub)))) return;
    yield* Effect.forkIn(hub.scope)(retire(hub));
  });

// A newer proxy retires an older hub unless an OpenCode session is connected.
// OpenCode never restarts a failed local server, so replacing the hub would end
// its watches for good. The close and the decision run in one synchronous step.
const decide = (hub: Hub, hello: { readonly name: Name; readonly version: string }): Reply => {
  if (hub.state.retiring) return { status: "replaced" };
  if (Bun.semver.order(hello.version, version) > 0 && hub.state.opencode === 0) {
    hub.state.retiring = true;
    hub.entry.close();
    return { status: "replaced" };
  }
  if (hello.name === "opencode") hub.state.opencode += 1;
  return { status: "accepted", version };
};

const connection = (socket: net.Socket, hub: Hub) => {
  let held = false;
  return Effect.scopedWith((scope) =>
    Effect.gen(function* () {
      const hello = yield* readHello(socket);
      if ("reason" in hello) {
        yield* Effect.logWarning(`closed before its harness arrived: ${hello.reason}`);
        return;
      }
      yield* Effect.logInfo(`connection opened: ${hello.name} ${hello.version}`);

      const reply = yield* Effect.sync(() => decide(hub, hello));
      socket.write(encodeReply(reply));
      if (reply.status === "replaced") {
        // The proxy waits for this socket to close before it finds the new hub.
        // Leaving it open, unref'd, closes it only as this process exits.
        held = true;
        hub.sockets.delete(socket);
        socket.unref();
        yield* Effect.logInfo(`replacing hub ${version} for ${hello.name} ${hello.version}`);
        yield* Deferred.succeed(hub.idle, undefined);
        yield* Effect.never;
      }
      if (hello.name === "opencode") {
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            hub.state.opencode -= 1;
          }),
        );
      }

      const input = new PassThrough();
      input.write(hello.rest);
      socket.pipe(input);
      socket.once("close", () => input.end());

      const transport = new StdioServerTransport(input, socket);
      const closed = yield* Deferred.make<void>();
      const server = yield* Server.make({ harness: hello.name, scope, watches: hub.watches });
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
      Effect.ensuring(
        Effect.sync(() => {
          if (!held) socket.destroy();
        }),
      ),
      Effect.onExit(() => Effect.logInfo("connection closed")),
    ),
  );
};

const serve = (hub: Hub) =>
  Effect.gen(function* () {
    while (true) {
      const socket = yield* Queue.take(hub.queue);
      yield* Effect.forkScoped(
        connection(socket, hub).pipe(
          Effect.ensuring(
            Effect.gen(function* () {
              hub.state.connections -= 1;
              yield* evaluate(hub);
            }),
          ),
        ),
      );
    }
  });

export const layer: Layer.Layer<Service, never, Dependencies> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const paths = yield* directories;
    yield* Effect.sync(() => ensureDirectory(paths.runtime));

    const queue = yield* Queue.unbounded<net.Socket>();
    const state: HubState = { connections: 0, opencode: 0, retiring: false };
    const idle = yield* Deferred.make<void>();
    const sockets = new Set<net.Socket>();
    const entry = net.createServer();

    // Attach the handler before listen. A connection accepted between listen and
    // the first handler waits in the queue instead of being dropped.
    entry.on("connection", (socket) => {
      // A connection can still arrive after the listener closes. Nothing would serve
      // it, and left open it keeps this process alive while its proxy waits.
      if (state.retiring) {
        socket.destroy();
        return;
      }
      sockets.add(socket);
      socket.once("close", () => sockets.delete(socket));
      state.connections += 1;
      Queue.offerUnsafe(queue, socket);
    });

    if (!(yield* bind(entry, paths.runtime))) return Service.of({ done: Effect.void });

    // close() unlinks the socket. A replacement hub can bind the path after our
    // close, so nothing may remove it again.
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        state.retiring = true;
        if (entry.listening) entry.close();
        for (const socket of sockets) socket.destroy();
      }),
    );

    const hub: Hub = {
      entry,
      queue,
      state,
      sockets,
      scope: yield* Effect.scope,
      idle,
      watches: new Map(),
    };
    yield* Effect.forkScoped(serve(hub));
    yield* Effect.forkIn(hub.scope)(
      Effect.sleep(STARTUP_GRACE).pipe(Effect.andThen(evaluate(hub))),
    );
    return Service.of({ done: Deferred.await(hub.idle) });
  }),
);

const LarkLayers = LarkSocket.layer.pipe(Layer.provideMerge(LarkAuth.layer));
const GitlabLayers = Layer.merge(GitlabSocket.layer, GitlabClient.layer).pipe(
  Layer.provideMerge(GitlabAuth.layer),
);
const CodemagicLayers = Layer.merge(CodemagicSocket.layer, CodemagicClient.layer);
const SourceLayers = Layer.mergeAll(LarkLayers, GitlabLayers, CodemagicLayers).pipe(
  Layer.provide(FetchHttpClient.layer),
);
const RegistryLayer = Registry.layer(
  Effect.all([Lark.source, Gitlab.source, Codemagic.source]),
).pipe(Layer.provide(SourceLayers));
const CoreLayer = Layer.mergeAll(Feeds.layer, DecisionModel.layer, RegistryLayer);

export const main = () => {
  const program = Effect.gen(function* () {
    const service = yield* Service;
    yield* service.done;
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
