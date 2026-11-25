import { expect, test } from "vitest";
import { Effect, Stream } from "effect";
import { makeSocket } from "../../src/codemagic/socket.ts";

const startServer = (options: { readonly refuse?: boolean } = {}) => {
  let connections = 0;
  const server = Bun.serve({
    port: 0,
    fetch(request, server) {
      return server.upgrade(request) ? undefined : new Response(null, { status: 400 });
    },
    websocket: {
      open(socket) {
        socket.send(
          '0{"sid":"engine","upgrades":[],"pingInterval":25000,"pingTimeout":20000,"maxPayload":1000000}',
        );
      },
      message(socket, data) {
        if (String(data) !== "40") return;
        if (options.refuse === true) {
          socket.send('44{"message":"Connection rejected by server"}');
          return;
        }
        connections += 1;
        socket.send('40{"sid":"socket"}');
        if (connections === 1) socket.send("41");
      },
    },
  });
  return {
    url: `ws://localhost:${server.port}`,
    connections: () => connections,
    stop: () => server.stop(true),
  };
};

const token = Effect.succeed("test-token");

test("reconnects after Codemagic closes the connection itself", async () => {
  const server = startServer();
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const socket = yield* makeSocket({ url: server.url, token });
        const session = yield* socket.subscribe;
        yield* session.health.pipe(
          Stream.filter((health) => health._tag === "Live" && server.connections() === 2),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("4 seconds"),
        );
      }).pipe(Effect.scoped),
    );
  } finally {
    server.stop();
  }
});

test("rejects the watch when Codemagic refuses the token", async () => {
  const server = startServer({ refuse: true });
  try {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const socket = yield* makeSocket({ url: server.url, token });
          return yield* socket.subscribe.pipe(Effect.flip);
        }),
      ),
    );
    expect(error._tag).toBe("Source.Rejected");
    expect(error.message).toContain("CODEMAGIC_TOKEN");
  } finally {
    server.stop();
  }
});
