import { createRequire } from "node:module";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { BunServices } from "@effect/platform-bun";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, FileSystem, Schema, Scope, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { TransformInput } from "../src/transform.ts";
import { makeTransformLoader } from "../src/transform-loader.ts";

const input: TransformInput = {
  root: { chatId: "chat", rootId: "root" },
  events: [],
  history: { items: [], hasMore: false },
  result: { answers: [{ type: "predicate", name: "urgent", probability: 0.9 }] },
};
const source = (version: string) => `
import type { Transform } from "oc-lark-decisions/transform";
import { identity } from "effect/Function";
export default ((input) => identity("${version}:" + input.root.rootId + ":" +
input.result.answers.filter(answer => answer.type === "predicate" && answer.probability > 0.8).length)) satisfies Transform;
`;

it.live(
  "loads TypeScript with public type annotations, fresh paths, value imports, and scoped cleanup",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "relay-loader-test-" });
      const scope = yield* Scope.fork(yield* Scope.Scope);
      const loadTransform = yield* makeTransformLoader.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          makeTempDirectoryScoped: (options) =>
            fs.makeTempDirectoryScoped({ ...options, directory: root }),
        }),
      );
      const first = yield* loadTransform({ source: source("v1") }).pipe(Scope.provide(scope));
      const second = yield* loadTransform({ source: source("v2") }).pipe(Scope.provide(scope));
      expect(yield* first.run(input)).toBe("v1:root:1");
      expect(yield* second.run(input)).toBe("v2:root:1");
      expect(yield* first.run(input)).toBe("v1:root:1");
      const created = yield* fs.readDirectory(root);
      expect(created).toHaveLength(2);
      for (const name of created) {
        expect((yield* fs.readDirectory(`${root}/${name}`)).toSorted()).toEqual([
          "node_modules",
          "package.json",
          "source.ts",
        ]);
        expect(yield* fs.exists(`${root}/${name}/node_modules/effect/package.json`)).toBe(true);
      }
      yield* Scope.close(scope, Exit.void);
      for (const name of created) expect(yield* fs.exists(`${root}/${name}`)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("filters an urgent predicate using the public TransformInput schema", () =>
  Effect.gen(function* () {
    const loadTransform = yield* makeTransformLoader;
    const transform = yield* loadTransform({
      source: `
export default ({ root, events, result }) => {
  const urgent = result.answers.some(
    (answer) => answer.type === "predicate" && answer.name === "urgent" && answer.probability >= 0.8,
  );
  if (!urgent) return null;
  const messages = events.map((event) => event.content).join("\\n");
  return \`Urgent Lark request in chat \${root.chatId}, conversation \${root.rootId}:\\n\${messages}\\nReview the request and suggest a next step.\`;
};
`,
    });
    const context = {
      root: { chatId: "chat", rootId: "root" },
      events: [
        {
          type: "im.message.receive_v1",
          chat_id: "chat",
          message_id: "message",
          root_id: "root",
          thread_id: "distinct-thread",
          content: "rendered urgent message",
        },
      ],
      history: {
        items: [{ message_id: "message", content: '{"text":"raw history"}' }],
        hasMore: true,
      },
    };
    const supplied = yield* Schema.decodeUnknownEffect(TransformInput)({
      ...context,
      result: {
        answers: [
          { type: "predicate", name: "urgent", probability: 0.9 },
          { type: "error", name: "unanswered", reason: "refused" },
        ],
      },
    });
    expect(yield* transform.run(supplied)).toBe(
      "Urgent Lark request in chat chat, conversation root:\nrendered urgent message\nReview the request and suggest a next step.",
    );
    for (const answer of [
      { type: "predicate", name: "urgent", probability: 0.79 },
      { type: "predicate", name: "different", probability: 0.99 },
      { type: "error", name: "urgent", reason: "refused" },
    ]) {
      const skipped = yield* Schema.decodeUnknownEffect(TransformInput)({
        ...context,
        result: { answers: [answer] },
      });
      expect(yield* transform.run(skipped)).toBeNull();
    }
    const asyncText = yield* loadTransform({
      source: "export default async (input) => input.root.rootId",
    });
    expect(yield* asyncText.run(supplied)).toBe("root");
    const asyncNull = yield* loadTransform({ source: "export default async () => null" });
    expect(yield* asyncNull.run(supplied)).toBeNull();
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("resolves packaged dependencies when the plugin has no local node_modules directory", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "relay-installed-test-" });
    const plugin = `${root}/packages/oc-lark-decisions`;
    yield* fs.makeDirectory(plugin, { recursive: true });
    yield* fs.copy(fileURLToPath(new URL("../src", import.meta.url)), `${plugin}/src`);
    yield* fs.writeFileString(
      `${plugin}/package.json`,
      '{"name":"oc-lark-decisions","type":"module"}',
    );
    const require = createRequire(import.meta.url);
    yield* fs.symlink(
      dirname(dirname(require.resolve("effect/package.json"))),
      `${root}/node_modules`,
    );
    expect(yield* fs.exists(`${plugin}/node_modules`)).toBe(false);
    const runner = `${root}/runner.ts`;
    yield* fs.writeFileString(
      runner,
      `
import { Effect } from "effect";
import { BunServices } from "@effect/platform-bun";
import { makeTransformLoader } from "./packages/oc-lark-decisions/src/transform-loader.ts";
await Effect.runPromise(Effect.gen(function* () {
  const loadTransform = yield* makeTransformLoader;
  const candidate = yield* loadTransform({ source: ${JSON.stringify(source("hoisted"))} });
  const result = yield* candidate.run(${JSON.stringify(input)});
  if (result !== "hoisted:root:1") throw new Error("Wrong installed result: " + result);
}).pipe(Effect.scoped, Effect.provide(BunServices.layer)));
process.stdout.write("installed loader passed");
`,
    );
    const process = yield* spawner.spawn(ChildProcess.make(globalThis.process.execPath, [runner]));
    const [stdout, stderr, code] = yield* Effect.all(
      [
        process.stdout.pipe(Stream.decodeText(), Stream.mkString),
        process.stderr.pipe(Stream.decodeText(), Stream.mkString),
        process.exitCode,
      ],
      { concurrency: "unbounded" },
    );
    expect({ code, stderr }).toEqual({ code: 0, stderr: "" });
    expect(stdout).toBe("installed loader passed");
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("accepts semantic TypeScript mismatches and validates resolved output at runtime", () =>
  Effect.gen(function* () {
    const loadTransform = yield* makeTransformLoader;
    const mismatch = yield* loadTransform({
      source: `import type { Transform } from "oc-lark-decisions/transform";
const value: number = "semantic mismatch accepted";
export default (() => value) satisfies Transform;`,
    });
    expect(yield* mismatch.run(input)).toBe("semantic mismatch accepted");
    const nullTransform = yield* loadTransform({ source: "export default () => null" });
    expect(yield* nullTransform.run(input)).toBeNull();
    const invalid = yield* loadTransform({
      source: `import type { Transform } from "oc-lark-decisions/transform";
export default (() => 42) satisfies Transform;`,
    });
    expect((yield* invalid.run(input).pipe(Effect.result))._tag).toBe("Failure");
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live(
  "rejects syntax, dependency, initialization, missing-export, and nonfunction-export failures",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "relay-loader-failure-test-" });
      const loadTransform = yield* makeTransformLoader.pipe(
        Effect.provideService(FileSystem.FileSystem, {
          ...fs,
          makeTempDirectoryScoped: (options) =>
            fs.makeTempDirectoryScoped({ ...options, directory: root }),
        }),
      );
      for (const source of [
        "export default ( => ;",
        'import "definitely-not-installed-relay-example"; export default () => null',
        "export default 42",
        "export const notDefault = () => null",
        'throw new Error("module load failed"); export default () => null',
        'await Promise.reject(new Error("module initialization rejected")); export default () => null',
      ]) {
        const result = yield* loadTransform({ source }).pipe(Effect.scoped, Effect.result);
        expect(result._tag).toBe("Failure");
        if (result._tag === "Failure") {
          expect(result.failure.operation).toBe("load");
          expect(String(result.failure.cause).length).toBeGreaterThan(0);
        }
        expect(yield* fs.readDirectory(root)).toEqual([]);
      }
    }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);

it.live("fails only the invocation for thrown, rejected, or invalid resolved outputs", () =>
  Effect.gen(function* () {
    const loadTransform = yield* makeTransformLoader;
    for (const source of [
      "export default () => { throw new Error('thrown'); }",
      "export default async () => { throw new Error('rejected'); }",
      "export default () => 42",
      "export default async () => ({ invalid: true })",
      "export default () => undefined",
    ]) {
      const loaded = yield* loadTransform({ source });
      expect((yield* loaded.run(input).pipe(Effect.exit))._tag).toBe("Failure");
    }
  }).pipe(Effect.scoped, Effect.provide(BunServices.layer)),
);
