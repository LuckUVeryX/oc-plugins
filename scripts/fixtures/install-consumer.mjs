import assert from "node:assert/strict";
import { readFile, realpath } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Effect, Stream } from "effect";
import { TestClock } from "effect/testing";

const expected = JSON.parse(await readFile(new URL("./expected.json", import.meta.url), "utf8"));
const resolved = new Map();

async function dependency(from, name, entry) {
  let directory = dirname(fileURLToPath(import.meta.resolve(entry, pathToFileURL(from).href)));
  while (true) {
    try {
      const manifest = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
      if (manifest.name === name) {
        assert.equal(manifest.version, expected.versions[name], `${name}: installed version`);
        const location = await realpath(directory);
        assert(location.startsWith(process.cwd() + "/"), `${name}: resolved outside consumer`);
        if (resolved.has(name))
          assert.equal(location, resolved.get(name), `${name}: duplicate family`);
        resolved.set(name, location);
        return;
      }
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const parent = dirname(directory);
    assert.notEqual(parent, directory, `${name}: package manifest not found`);
    directory = parent;
  }
}

async function located(from, name) {
  let directory = dirname(from);
  while (true) {
    const candidate = join(directory, "node_modules", name, "package.json");
    try {
      await readFile(candidate);
      return candidate;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    const parent = dirname(directory);
    assert.notEqual(parent, directory, `${name}: package not installed`);
    directory = parent;
  }
}

async function verifyExecutable(name) {
  const signal = AbortSignal.timeout(10_000);
  const child = Bun.spawn(
    [
      process.execPath,
      "--preload",
      "./lark-sdk-preload.mjs",
      join("node_modules", ".bin", name),
      "opencode",
    ],
    {
      cwd: process.cwd(),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      signal,
      env: {
        ...process.env,
        EVENTS_MCP_HUB_DIR: join(process.cwd(), "hub"),
        LARK_APP_ID: "cli_0000000000000001",
        LARK_APP_SECRET: "offline",
      },
    },
  );
  const stderr = new Response(child.stderr).text();
  child.stdin.write(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 2,
      method: "events/list",
      params: {
        _meta: {
          "io.modelcontextprotocol/protocolVersion": "2026-07-28",
          "io.modelcontextprotocol/clientInfo": { name: "install-test", version: "0.0.0" },
          "io.modelcontextprotocol/clientCapabilities": {},
        },
      },
    }) + "\n",
  );
  child.stdin.flush();
  const reader = child.stdout.getReader();
  const decoder = new TextDecoder();
  let text = "";
  while (!text.includes('"id":2') || !text.endsWith("\n")) {
    const chunk = await reader.read();
    if (chunk.done)
      throw new Error(`${name}: closed before catalog response\n${text}\n${await stderr}`);
    text += decoder.decode(chunk.value, { stream: true });
  }
  const responses = text
    .trim()
    .split("\n")
    .map((line) => JSON.parse(line));
  const events = responses.find((response) => response.id === 2)?.result?.events ?? [];
  assert.equal(events.length, 23);
  assert(events.some((event) => event.name === "lark.im.message.receive_v1"));
  assert(events.some((event) => event.name === "gitlab.ciPipelineStatusesUpdated"));
  assert(events.some((event) => event.name === "gitlab.noteable.notes_changed"));
  assert(events.some((event) => event.name === "codemagic.buildStatusUpdated"));
  child.stdin.end();
  const exited = await child.exited;
  assert(!signal.aborted, `${name}: executable did not stop after stdin EOF`);
  const diagnostics = await stderr;
  assert.equal(exited, 0, `${name}: executable exited ${exited}\n${diagnostics}`);
  assert(!diagnostics.includes("offline SDK started"), `${name}: idle server started the SDK`);
  assert(
    responses.every((response) => response.jsonrpc === "2.0"),
    `${name}: non-MCP stdout`,
  );

  const closedSignal = AbortSignal.timeout(10_000);
  const closed = Bun.spawn(
    [
      process.execPath,
      "--preload",
      "./lark-sdk-preload.mjs",
      join("node_modules", ".bin", name),
      "opencode",
    ],
    {
      cwd: process.cwd(),
      stdin: "pipe",
      stdout: "pipe",
      stderr: "pipe",
      signal: closedSignal,
      env: {
        ...process.env,
        EVENTS_MCP_HUB_DIR: join(process.cwd(), "hub"),
        LARK_APP_ID: "cli_0000000000000001",
        LARK_APP_SECRET: "offline",
      },
    },
  );
  const closedStderr = new Response(closed.stderr).text();
  closed.stdin.end();
  const closedExit = await closed.exited;
  assert(!closedSignal.aborted, `${name}: executable did not stop when stdin was already closed`);
  const closedDiagnostics = await closedStderr;
  assert.equal(
    closedExit,
    0,
    `${name}: executable exited ${closedExit} on an already-closed stdin\n${closedDiagnostics}`,
  );
  assert(
    !closedDiagnostics.includes("offline SDK started"),
    `${name}: idle server started the SDK on an already-closed stdin`,
  );
}

for (const manifest of expected.packages) {
  if (manifest.bin) {
    const entry = fileURLToPath(import.meta.resolve(`${manifest.name}/package.json`));
    for (const field of ["dependencies", "peerDependencies", "optionalDependencies"]) {
      for (const name of Object.keys(manifest[field] ?? {})) {
        await dependency(entry, name, await located(entry, name));
      }
    }
    for (const name of Object.keys(manifest.bin)) await verifyExecutable(name);
    console.log(`${manifest.name}: installed executable exits cleanly on stdin EOF`);
    continue;
  }
  const module = await import(manifest.name);
  const plugin = module.default;
  const entry = fileURLToPath(import.meta.resolve(manifest.name));
  assert.equal(plugin.id, manifest.name);
  assert.equal(typeof plugin.effect, "function");
  await dependency(entry, "effect", "effect");
  await dependency(entry, "@opencode/plugin", "@opencode/plugin/effect");
  if (manifest.peerDependencies["@opencode/schema"]) {
    await dependency(entry, "@opencode/schema", "@opencode/schema/session");
  }
  const exports =
    typeof manifest.exports === "string" ? { ".": manifest.exports } : manifest.exports;
  for (const subpath of Object.keys(exports)) {
    await import(subpath === "." ? manifest.name : manifest.name + subpath.slice(1));
  }

  if (manifest.name === "oc-fallback") {
    const fallbacks = module.decodeFallbacks({ fallbacks: { "a/b": "c/d#ignored" } });
    const input = {
      errorType: "provider.free-limit",
      kind: "primary",
      model: { providerID: "a", id: "b", variant: "high" },
      fallbacks,
    };
    assert.deepEqual(module.fallbackFor(input), { providerID: "c", id: "d", variant: "high" });
    assert.equal(module.fallbackFor({ ...input, kind: "compaction" }), undefined);
    assert.equal(module.fallbackFor({ ...input, errorType: "provider.rate-limit" }), undefined);
    assert.throws(() => module.decodeFallbacks({ fallbacks: { invalid: "c/d" } }));
    const hooks = new Map();
    const switched = [];
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* plugin.effect({
            options: { fallbacks: { "a/b": "c/d" } },
            session: {
              hook: (name, handler) => Effect.sync(() => hooks.set(name, handler)),
              switchModel: (request) => Effect.sync(() => switched.push(request)),
            },
          });
          yield* hooks.get("model.request")({ sessionID: "ses_test", kind: "primary" });
          const event = {
            sessionID: "ses_test",
            model: input.model,
            error: { type: "provider.free-limit" },
          };
          yield* hooks.get("retry")(event);
          assert.deepEqual(event.decision, { retry: true, delay: 0 });
          assert.deepEqual(switched, [
            {
              sessionID: "ses_test",
              model: { providerID: "c", id: "d", variant: "high" },
            },
          ]);
        }),
      ),
    );
    console.log("fallback helpers, primary retry hook, variant, and switch passed");
  }

  if (manifest.name === "oc-idle") {
    const finished = (sessionID, input) => [
      {
        type: "session.step.ended",
        data: { sessionID, tokens: { input, output: 0, cache: { read: 0, write: 0 } } },
      },
      { type: "session.execution.succeeded", data: { sessionID } },
    ];
    const compacted = [];
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* plugin.effect({
            options: { idleMinutes: 1, minTokens: 10 },
            event: {
              subscribe: () =>
                Stream.fromIterable([
                  ...finished("ses_cancelled", 11),
                  { type: "session.execution.started", data: { sessionID: "ses_cancelled" } },
                  ...finished("ses_equal", 10),
                  ...finished("ses_child", 11),
                  ...finished("ses_large", 11),
                ]),
            },
            session: {
              get: ({ sessionID }) =>
                Effect.succeed({
                  parentID: sessionID === "ses_child" ? "ses_parent" : undefined,
                }),
              compact: ({ sessionID }) => Effect.sync(() => compacted.push(sessionID)),
            },
          });
          yield* TestClock.adjust("1 minute");
          assert.deepEqual(compacted, ["ses_large"]);
        }),
      ).pipe(Effect.provide(TestClock.layer())),
    );
    console.log("idle timer, cancellation, token threshold, and root-session filter passed");
  }
}
console.log("public exports, package contracts, and one compatible dependency family passed");
