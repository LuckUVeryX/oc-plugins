import { expect, it } from "@effect/vitest";
import { Session } from "@opencode/schema/session";
import { SessionEvent } from "@opencode/schema/session-event";
import { Tool } from "@opencode/schema/tool";
import {
  Context,
  Effect,
  Fiber,
  JsonSchema,
  Layer,
  Queue,
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

it.effect("registers exactly the native tools and scopes execution to the caller", () =>
  Effect.gen(function* () {
    const tools = new Map();
    const ctx = {
      options,
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
    };
    yield* plugin.effect(ctx);
    expect([...tools.keys()]).toEqual([
      "lark_watch",
      "lark_stop",
      "lark_status",
      "lark_transform_schema",
    ]);
    const context = { sessionID: Session.ID.make("sesCaller") };
    expect(yield* tools.get("lark_status").execute({}, context)).toEqual({
      output: { state: "stopped" },
    });
    expect(yield* tools.get("lark_stop").execute({}, context)).toEqual({
      output: { state: "stopped" },
    });
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
    const failed = yield* watch.execute(configuration, context).pipe(Effect.result);
    expect(failed._tag).toBe("Failure");
    if (failed._tag === "Failure") {
      expect(failed.failure).toBeInstanceOf(Tool.Error);
      expect(failed.failure.message).toContain("default");
    }
  }),
);

it.effect(
  "returns exactly the self-contained generated input and output transform schemas without starting a watch",
  () =>
    Effect.gen(function* () {
      const { directory, fs } = yield* controlledCLI(
        'printf called > "$(dirname "$0")/called"\nexit 77',
      );
      const tools = new Map();
      const ctx = {
        options,
        tool: {
          transform: (register) =>
            Effect.sync(() => {
              register({ add: (tool) => tools.set(tool.name, tool) });
              return { dispose: Effect.void };
            }),
        },
        event: { subscribe: () => Stream.never },
        experimental: { generate: { decisions: () => Effect.die("unexpected model call") } },
        session: { prompt: () => Effect.die("unexpected delivery") },
      };
      yield* plugin.effect(ctx);
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

it.effect("a successful native watch delivers only to its caller using the operator's model", () =>
  Effect.gen(function* () {
    yield* controlledCLI(`
if [ "$1" = event ]; then
  trap 'exit 0' TERM
  printf '%s\\n' '{"type":"im.message.receive_v1","chat_id":"chat","message_id":"message","content":"native event","create_time":"0"}'
  printf '[event] ready event_key=im.message.receive_v1\\n' >&2
  read waiting
else
  printf '%s\\n' '{"ok":true,"data":{"items":[{"message_id":"message","body":{"content":"native event"}}]}}'
fi`);
    const tools = new Map();
    const decisions = yield* Queue.unbounded();
    const prompts = yield* Queue.unbounded();
    const ctx = {
      options,
      tool: {
        transform: (register) =>
          Effect.sync(() => {
            register({ add: (tool) => tools.set(tool.name, tool) });
            return { dispose: Effect.void };
          }),
      },
      event: { subscribe: () => Stream.never },
      experimental: {
        generate: {
          decisions: (input) => Queue.offer(decisions, input).pipe(Effect.as({ answers: [] })),
        },
      },
      session: { prompt: (input) => Queue.offer(prompts, input) },
    };
    yield* plugin.effect(ctx);
    const watch = tools.get("lark_watch");
    const input = yield* Schema.decodeUnknownEffect(watch.input)({
      questions: [{ type: "predicate", name: "urgent", instructions: "Is this urgent?" }],
      transform: 'export default () => "literal native prompt"',
      debounceMilliseconds: 500,
      sessionID: "sesOther",
      model: { providerID: "agent-provider", id: "agent-model" },
      workers: 100,
    });
    const caller = { sessionID: Session.ID.make("sesCaller") };
    const result = yield* watch.execute(input, caller);
    expect(result.output.state).toBe("active");
    expect(result.output).not.toHaveProperty("diagnostic");
    expect(result.output.configuration).not.toHaveProperty("workers");
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
    yield* tools.get("lark_stop").execute({}, caller);
  }).pipe(Effect.provide(BunServices.layer)),
);

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

import { BunServices } from "@effect/platform-bun";
