import assert from "node:assert/strict";
import { readFile, realpath } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { Effect, Schema, Stream } from "effect";
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

for (const manifest of expected.packages) {
  const module = await import(manifest.name);
  const plugin = module.default;
  assert.equal(plugin.id, manifest.name === "oc-lark-decisions" ? "oc-lark-relay" : manifest.name);
  assert.equal(typeof plugin.effect, "function");
  const entry = fileURLToPath(import.meta.resolve(manifest.name));
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

  if (manifest.name === "oc-lark-decisions") {
    const { BunServices } = await import("@effect/platform-bun");
    const { TransformInput } = await import("oc-lark-decisions/transform");
    const { makeTransformLoader } = await import(
      pathToFileURL(join(dirname(entry), "transform-loader.ts")).href
    );
    const input = Schema.decodeUnknownSync(TransformInput)({
      root: { chatId: "chat", rootId: "root" },
      events: [],
      history: { items: [], hasMore: false },
      result: { answers: [{ type: "predicate", name: "urgent", probability: 0.9 }] },
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        const load = yield* makeTransformLoader;
        const transform = yield* load({
          source: `
import type { Transform } from "oc-lark-decisions/transform";
import { identity } from "effect/Function";
export default ((input) => identity("review:" + input.root.rootId)) satisfies Transform;
`,
        });
        assert.equal(yield* transform.run(input), "review:root");
      }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
    );
    console.log("decisions public schema and real Effect-importing transform loader passed");
  }
}
console.log("public exports, plugin IDs, and one compatible Effect/OpenCode family passed");
