import { homedir } from "node:os";
import { join } from "node:path";
import { Config, Effect, Schema } from "effect";
import { AppServer } from "./codex-app-server.ts";
import { instructions, listEventsInstructions, WakeFailed, type Tools } from "./harness.ts";

const ThreadId = Schema.Struct({
  threadId: Schema.String.annotate({
    description:
      'This Codex thread\'s id. Read it with printf %s "$CODEX_THREAD_ID" in your shell and pass the exact value. A wake starts a turn in this thread.',
  }),
});

// Codex runs one app-server daemon per Codex home and serves it on this Unix socket.
const socketPath = Effect.gen(function* () {
  const home = yield* Config.String("CODEX_HOME").pipe(
    Config.withDefault(join(homedir(), ".codex")),
    Effect.orDie,
  );
  return join(home, "app-server-control", "app-server-control.sock");
});

const threadIdOf = (session: Record<string, Schema.Json>) =>
  Schema.decodeUnknownEffect(ThreadId)(session).pipe(
    Effect.mapError((error) => new WakeFailed({ message: error.message })),
  );

export const harness: Tools = {
  _tag: "Tools",
  capabilities: {},
  instructions: instructions({
    arm: listEventsInstructions,
    wakes: [
      'Pass threadId to the watch tool. Read it in this thread\'s shell with printf %s "$CODEX_THREAD_ID" and pass the exact value. Codex does not tell this server which thread it serves, and a watch with a missing or unknown threadId fails.',
      "Wakes arrive as a new user turn in this thread. They are one-way. Act on the wake in that turn.",
    ],
  }),
  watchParams: ThreadId,
  check: (session) =>
    Effect.gen(function* () {
      const { threadId } = yield* threadIdOf(session);
      const path = yield* socketPath;
      yield* AppServer.checkThread(path, threadId);
    }),
  wake: (_, { session, text, watchId }) =>
    Effect.gen(function* () {
      const { threadId } = yield* threadIdOf(session);
      const path = yield* socketPath;
      yield* AppServer.queue(path, {
        threadId,
        text,
        clientUserMessageId: `${watchId}:${crypto.randomUUID()}`,
      });
    }),
};
