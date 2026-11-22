import { mkdirSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { Config, Effect, Option } from "effect";

const userId = () => {
  const uid = process.getuid?.();
  if (uid === undefined) throw new Error("events-mcp needs a POSIX user id (Linux or macOS)");
  return uid;
};

// Every harness reads the same runtime directory. Codex strips XDG_RUNTIME_DIR
// from an MCP server's environment, so the fallback to /run/user/<uid> is what
// keeps it on the same socket as OpenCode and Claude Code. macOS has no runtime
// directory, so the socket sits under the state directory.
export const directories = Effect.gen(function* () {
  const state = join(homedir(), ".local", "state", "events-hub");
  const override = yield* Config.option(Config.String("EVENTS_MCP_HUB_DIR"));
  if (Option.isSome(override) && override.value !== "")
    return { runtime: join(override.value, "run"), state: join(override.value, "state") };
  if (process.platform === "darwin") return { runtime: state, state };
  const xdg = yield* Config.option(Config.String("XDG_RUNTIME_DIR"));
  const uid = yield* Effect.sync(userId);
  const directory =
    Option.isSome(xdg) && xdg.value !== "" ? xdg.value : join("/run/user", String(uid));
  return { runtime: join(directory, "events-hub"), state };
}).pipe(Effect.orDie);

export const socketPath = (runtime: string) => join(runtime, "hub.sock");
export const lockPath = (runtime: string) => join(runtime, "locks");
export const logPath = (state: string) => join(state, "hub.log");

export const ensureDirectory = (path: string) => {
  try {
    mkdirSync(path, { recursive: true, mode: 0o700 });
  } catch (cause) {
    throw new Error(`events-mcp: cannot create the hub directory ${path}`, { cause });
  }
};
