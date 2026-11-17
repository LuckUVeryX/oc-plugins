import { expect, it } from "@effect/vitest";
import { Session } from "@opencode/schema/session";
import { SessionEvent } from "@opencode/schema/session-event";
import { Tool } from "@opencode/schema/tool";
import {
  Cause,
  Context,
  Effect,
  Fiber,
  JsonSchema,
  Layer,
  Queue,
  Ref,
  Schema,
  SchemaRepresentation,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import plugin from "../src/index.ts";
import { OpenCode, Options, openCodeLayer } from "../src/opencode.ts";
import { TransformInput } from "../src/transform.ts";
import { controlledCLI } from "./fixtures.ts";

const options = Schema.decodeUnknownSync(Options)({
  model: { providerID: "operator-provider", id: "operator-model" },
});

const makeStorage = () => {
  const values = new Map();
  return {
    values,
    get: (key) => Effect.sync(() => values.get(key)),
    set: (key, value) =>
      Effect.sync(() => {
        values.set(key, value);
      }),
    remove: (key) =>
      Effect.sync(() => {
        values.delete(key);
      }),
  };
};

const loadTools = Effect.fn("Test.loadTools")(function* (overrides = {}) {
  const tools = new Map();
  yield* plugin.effect({
    options,
    storage: makeStorage(),
    tool: {
      transform: (register) =>
        Effect.sync(() => {
          register({ add: (tool) => tools.set(tool.name, tool) });
          return { dispose: Effect.void };
        }),
    },
    event: { subscribe: () => Stream.never },
    experimental: { generate: { decisions: () => Effect.die("unexpected decisions") } },
    session: { prompt: () => Effect.die("unexpected prompt") },
    ...overrides,
  });
  return tools;
});

it.effect("registers exactly the native tools and scopes execution to the caller", () =>
  Effect.gen(function* () {
    const storage = makeStorage();
    const unreadable = yield* Ref.make(false);
    const tools = yield* loadTools({
      storage: {
        ...storage,
        get: (key) =>
          Ref.get(unreadable).pipe(
            Effect.flatMap((failed) => (failed ? Effect.die("unreadable") : storage.get(key))),
          ),
      },
    });
    expect([...tools.keys()]).toEqual([
      "lark_watch",
      "lark_stop",
      "lark_status",
      "lark_history",
      "lark_transform_schema",
    ]);
    const context = { sessionID: Session.ID.make("sesCaller") };
    expect(yield* tools.get("lark_status").execute({}, context)).toEqual({
      output: { state: "stopped" },
    });
    expect(yield* tools.get("lark_stop").execute({}, context)).toEqual({
      output: { state: "stopped" },
    });
    const history = tools.get("lark_history");
    expect(Object.keys(history.input.fields)).toEqual([]);
    expect(yield* history.execute({ sessionID: "foreign" }, context)).toEqual({ output: [] });
    yield* Ref.set(unreadable, true);
    const readError = yield* history.execute({}, context).pipe(Effect.flip);
    expect(readError).toBeInstanceOf(Tool.Error);
    expect(readError.message).toContain("unreadable");
    const watch = tools.get("lark_watch");
    const configuration = yield* Schema.decodeUnknownEffect(watch.input)({
      questions: [{ type: "predicate", name: "urgent", instructions: "Is this urgent?" }],
      transform: "export default 42",
      sessionID: "not-the-caller",
      model: { providerID: "agent", id: "override" },
      workers: 100,
    });
    expect(configuration).not.toHaveProperty("sessionID");
    expect(configuration).not.toHaveProperty("model");
    expect(configuration).not.toHaveProperty("workers");
    expect(tools.get("lark_status").description).not.toContain("diagnostic");
    const watchError = yield* watch.execute(configuration, context).pipe(Effect.flip);
    expect(watchError).toBeInstanceOf(Tool.Error);
    expect(watchError.message).toContain("default");
  }),
);

it.effect(
  "returns exactly the self-contained generated input and output transform schemas without starting a watch",
  () =>
    Effect.gen(function* () {
      const { directory, fs } = yield* controlledCLI(
        'printf called > "$(dirname "$0")/called"\nexit 77',
      );
      const tools = yield* loadTools();
      const caller = { sessionID: Session.ID.make("sesCaller") };
      const { output: help } = yield* tools.get("lark_transform_schema").execute({}, caller);
      expect(Object.keys(help).sort()).toEqual(["inputSchema", "outputSchema"]);
      const document = Schema.toJsonSchemaDocument(TransformInput);
      expect(help.inputSchema).toEqual({
        $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
        ...document.schema,
        $defs: document.definitions,
      });
      expect(Object.keys(help.inputSchema.$defs).length).toBeGreaterThan(0);
      const outputDocument = Schema.toJsonSchemaDocument(Schema.NullOr(Schema.String));
      expect(help.outputSchema).toEqual({
        $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
        ...outputDocument.schema,
        $defs: outputDocument.definitions,
      });
      const inputSchema = SchemaRepresentation.fromJsonSchemaDocument(
        JsonSchema.fromSchemaDraft2020_12(help.inputSchema),
      );
      const outputSchema = SchemaRepresentation.fromJsonSchemaDocument(
        JsonSchema.fromSchemaDraft2020_12(help.outputSchema),
      );
      const supplied = yield* Schema.decodeUnknownEffect(TransformInput)({
        root: { chatId: "chat", rootId: "root" },
        events: [
          {
            type: "im.message.receive_v1",
            chat_id: "chat",
            message_id: "message",
            root_id: "root",
            thread_id: "distinct-thread",
            content: "rendered message",
          },
        ],
        history: {
          items: [{ message_id: "message", content: '{"text":"raw history"}' }],
          hasMore: true,
        },
        result: {
          answers: [
            { type: "predicate", name: "urgent", probability: 0.9 },
            {
              type: "choice",
              name: "selected",
              choice: true,
              probabilities: [
                { value: true, probability: 0.8 },
                { value: false, probability: 0.2 },
              ],
              confidence: 0.8,
            },
            {
              type: "score",
              name: "severity",
              score: 0.75,
              probabilities: [
                { value: 0, label: "low", probability: 0.25 },
                { value: 1, label: "high", probability: 0.75 },
              ],
              confidence: 0.8,
            },
            { type: "error", name: "unanswered", reason: "refused", message: "fixture refusal" },
          ],
          cost: 0.01,
          tokens: { input: 10, output: 3 },
          native: { vendor: "fixture" },
        },
      });
      const encoded = Schema.encodeSync(Schema.toCodecJson(TransformInput))(supplied);
      expect(yield* Schema.decodeUnknownEffect(inputSchema)(encoded)).toEqual(encoded);
      expect(Schema.is(inputSchema)({ ...encoded, root: { chatId: "chat" } })).toBe(false);
      for (const output of ["literal prompt", null])
        expect(Schema.is(outputSchema)(output)).toBe(true);
      for (const output of [42, {}, undefined]) expect(Schema.is(outputSchema)(output)).toBe(false);
      expect(yield* tools.get("lark_status").execute({}, caller)).toEqual({
        output: { state: "stopped" },
      });
      expect(yield* fs.exists(`${directory}/called`)).toBe(false);
    }).pipe(Effect.provide(BunServices.layer)),
);

it.effect("maps native session.deleted events to their session IDs", () =>
  Effect.gen(function* () {
    const events = yield* Queue.unbounded();
    const subscriptions = [];
    const ctx = {
      experimental: { generate: { decisions: () => Effect.die("unexpected decisions") } },
      session: { prompt: () => Effect.die("unexpected prompt") },
      event: {
        subscribe: (input) => {
          subscriptions.push(input);
          return Stream.fromQueue(events);
        },
      },
    };
    const service = Context.get(yield* Layer.build(openCodeLayer(ctx, options)), OpenCode);
    const deleted = yield* service.deletedSessions.pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.forkScoped,
    );
    const event = yield* Schema.decodeUnknownEffect(SessionEvent.Deleted)({
      id: "evt_deleted",
      created: 123,
      type: "session.deleted",
      durable: { aggregateID: "sesDeleted", seq: 1, version: SessionEvent.Deleted.durable.version },
      data: { sessionID: "sesDeleted" },
    });
    yield* Queue.offer(events, event);
    expect(yield* Fiber.join(deleted)).toEqual([Session.ID.make("sesDeleted")]);
    expect(subscriptions).toEqual([{ types: ["session.deleted"] }]);
  }),
);

for (const storageFailure of [undefined, "set"]) {
  it.effect(
    `a native watch preserves delivery and session behavior with storage ${storageFailure ?? "available"}`,
    () =>
      Effect.gen(function* () {
        yield* controlledCLI(`
if [ "$1" = event ]; then
  trap 'exit 0' TERM
  printf '%s\\n' '{"type":"im.message.receive_v1","chat_id":"chat","message_id":"message","content":"native event","create_time":"0"}'
  printf '%s\\n' '{"type":"im.message.receive_v1","chat_id":"chat","message_id":"next","content":"next event","create_time":"0"}'
  printf '[event] ready event_key=im.message.receive_v1\\n' >&2
  read waiting
else
  printf '%s\\n' '{"ok":true,"data":{"items":[{"message_id":"message","body":{"content":"native event"}},{"message_id":"next","body":{"content":"next event"}}]}}'
fi`);
        const decisions = yield* Queue.unbounded();
        const prompts = yield* Queue.unbounded();
        const storage = makeStorage();
        const tools = yield* loadTools({
          storage: {
            get: storage.get,
            set: (key, value) =>
              storageFailure === "set" ? Effect.die("set defect") : storage.set(key, value),
            remove: storage.remove,
          },
          experimental: {
            generate: {
              decisions: (input) => Queue.offer(decisions, input).pipe(Effect.as({ answers: [] })),
            },
          },
          session: { prompt: (input) => Queue.offer(prompts, input) },
        });
        const watch = tools.get("lark_watch");
        const input = yield* Schema.decodeUnknownEffect(watch.input)({
          questions: [{ type: "predicate", name: "urgent", instructions: "Is this urgent?" }],
          transform: 'export default () => "literal native prompt"',
          debounceMilliseconds: 500,
        });
        const caller = { sessionID: Session.ID.make("sesCaller") };
        const result = yield* watch.execute(input, caller);
        expect(result.output.state).toBe("active");
        expect(
          yield* tools.get("lark_status").execute({}, { sessionID: Session.ID.make("sesOther") }),
        ).toEqual({
          output: { state: "stopped" },
        });
        yield* TestClock.adjust(0);
        yield* TestClock.adjust("500 millis");
        expect((yield* Queue.take(decisions)).model).toEqual(options.model);
        expect(yield* Queue.take(prompts)).toEqual({
          sessionID: "sesCaller",
          text: "literal native prompt",
          delivery: "queue",
        });
        expect((yield* Queue.take(prompts)).text).toBe("literal native prompt");
        expect((yield* tools.get("lark_status").execute({}, caller)).output.state).toBe("active");
        yield* tools.get("lark_stop").execute({}, caller);
        const history = (yield* tools.get("lark_history").execute({}, caller)).output;
        if (storageFailure) {
          expect(history).toEqual([]);
          return;
        }
        const expectedRecord = {
          completedAt: 500,
          root: { chatId: "chat", rootId: "message" },
          eventCount: 1,
          firstMessageId: "message",
          lastMessageId: "message",
          answers: [],
          outcome: { kind: "queued" },
        };
        expect(history).toEqual([
          {
            ...expectedRecord,
            root: { chatId: "chat", rootId: "next" },
            firstMessageId: "next",
            lastMessageId: "next",
          },
          expectedRecord,
        ]);
        expect(
          yield* tools.get("lark_history").execute({}, { sessionID: Session.ID.make("sesOther") }),
        ).toEqual({ output: [] });
      }).pipe(Effect.provide(BunServices.layer)),
  );
}

it.effect("the native adapter fixes the model and requests literal queued prompts", () =>
  Effect.gen(function* () {
    const seen = [];
    const result = {
      answers: [{ type: "error", name: "failed", reason: "provider answer error" }],
    };
    const ctx = {
      experimental: {
        generate: {
          decisions: (input) =>
            Effect.sync(() => {
              seen.push(input);
              return result;
            }),
        },
      },
      session: {
        prompt: (input) =>
          Effect.sync(() => {
            seen.push(input);
          }),
      },
      event: { subscribe: () => Stream.never },
    };
    const input = {
      questions: [{ type: "predicate", name: "urgent", instructions: "Is this urgent?" }],
      input: [{ type: "text", text: "evidence" }],
    };
    const service = Context.get(yield* Layer.build(openCodeLayer(ctx, options)), OpenCode);
    expect(yield* service.decide(input)).toBe(result);
    yield* service.queue({ sessionID: Session.ID.make("sesCaller"), text: "not JSON encoded" });
    expect(seen).toEqual([
      { ...input, model: options.model },
      { sessionID: "sesCaller", text: "not JSON encoded", delivery: "queue" },
    ]);
  }),
);

const batchRecord = (message = "message") => ({
  completedAt: 123,
  root: { chatId: "chat", rootId: "root" },
  eventCount: 1,
  firstMessageId: message,
  lastMessageId: message,
  answers: [{ type: "error", name: "question", reason: "refused" }],
  outcome: { kind: "skipped" },
});

it.effect(
  "persists bounded newest-first history across fresh native layers and isolates sessions",
  () =>
    Effect.gen(function* () {
      const storage = makeStorage();
      const ctx = {
        storage,
        event: { subscribe: () => Stream.never },
      };
      const build = Effect.map(Layer.build(openCodeLayer(ctx, options)), (context) =>
        Context.get(context, OpenCode),
      );
      const first = yield* build;
      const a = Session.ID.make("sesA");
      const b = Session.ID.make("sesB");
      yield* first.recordBatch(b, batchRecord("other"));
      for (let index = 0; index < 105; index++) {
        yield* first.recordBatch(a, batchRecord(String(index)));
      }
      const fresh = yield* build;
      const records = yield* fresh.history(a);
      expect(records.length).toBe(100);
      expect(records[0].firstMessageId).toBe("104");
      expect(records.at(-1).firstMessageId).toBe("5");
      expect((yield* fresh.history(b))[0].firstMessageId).toBe("other");
      for (let index = 0; index < 10; index++) {
        yield* fresh.recordBatch(a, batchRecord(`${index}${"界😀".repeat(1500)}`));
      }
      const unicode = yield* fresh.history(a);
      expect(unicode.length).toBeLessThan(10);
      expect(new TextEncoder().encode(JSON.stringify(unicode)).byteLength).toBeLessThanOrEqual(
        65536,
      );
      expect(unicode[0].firstMessageId.startsWith("9")).toBe(true);
      yield* fresh.recordBatch(a, batchRecord("界".repeat(30_000)));
      expect(yield* fresh.history(a)).toEqual(unicode);
      yield* fresh.removeHistory(a);
      expect(yield* first.history(a)).toEqual([]);
      expect((yield* first.history(b)).length).toBe(1);
    }),
);

it.effect(
  "contains native storage defects without overwriting unreadable history and reports read errors",
  () =>
    Effect.gen(function* () {
      const storage = makeStorage();
      let failure;
      const ctx = {
        storage: {
          get: (key) => (failure === "get" ? Effect.die("get defect") : storage.get(key)),
          set: (key, value) =>
            failure === "set" ? Effect.die("set defect") : storage.set(key, value),
          remove: (key) =>
            failure === "remove" ? Effect.die("remove defect") : storage.remove(key),
        },
        event: { subscribe: () => Stream.never },
      };
      const service = Context.get(yield* Layer.build(openCodeLayer(ctx, options)), OpenCode);
      const sessionID = Session.ID.make("sesA");
      yield* service.recordBatch(sessionID, batchRecord());
      const original = [...storage.values.entries()];
      for (const operation of ["get", "set", "remove"]) {
        failure = operation;
        yield* service.recordBatch(sessionID, batchRecord("new"));
        if (operation === "get") {
          const error = yield* service.history(sessionID).pipe(Effect.flip);
          expect(error.message).toContain("get defect");
        }
        if (operation === "remove") yield* service.removeHistory(sessionID);
        if (operation !== "remove") expect([...storage.values.entries()]).toEqual(original);
      }
      failure = undefined;
      const key = [...storage.values.keys()][0];
      storage.values.set(key, { unknown: "future format" });
      const error = yield* service.history(sessionID).pipe(Effect.flip);
      expect(error.operation).toBe("history");
      yield* service.recordBatch(sessionID, batchRecord());
      expect(storage.values.get(key)).toEqual({ unknown: "future format" });
    }),
);

it.effect("preserves cancellation during storage reads and batch writes", () =>
  Effect.gen(function* () {
    const entered = yield* Queue.unbounded();
    const ctx = {
      storage: {
        get: () => Queue.offer(entered, undefined).pipe(Effect.andThen(Effect.never)),
        set: () => Effect.die("unexpected write"),
      },
      event: { subscribe: () => Stream.never },
    };
    const service = Context.get(yield* Layer.build(openCodeLayer(ctx, options)), OpenCode);
    const sessionID = Session.ID.make("sesA");
    for (const operation of [
      service.history(sessionID),
      service.recordBatch(sessionID, batchRecord()),
    ]) {
      const fiber = yield* operation.pipe(Effect.forkScoped);
      yield* Queue.take(entered);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      expect(exit._tag).toBe("Failure");
      expect(Cause.hasInterrupts(exit.cause)).toBe(true);
    }
  }),
);

import { BunServices } from "@effect/platform-bun";
