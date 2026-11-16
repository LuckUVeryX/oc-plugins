import { expect, it } from "@effect/vitest";
import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { Options } from "../src/opencode.ts";
import { WatchInput, WatchStatus } from "../src/relay.ts";
import { LarkEvent } from "../src/transform.ts";

const input = {
  questions: [{ type: "predicate", name: "urgent", instructions: "Is this urgent?" }],
  transform: "export default () => null",
};

it("exports parameter guidance in the tool's input JSON Schema", () => {
  const document = Schema.toJsonSchemaDocument(WatchInput);
  expect(document.schema).toMatchObject({
    properties: {
      input: { description: expect.stringContaining("session's conversation") },
      transform: { description: expect.stringContaining("default-exported function") },
      timeoutSeconds: { description: expect.stringContaining("Watch lifetime") },
      debounceMilliseconds: { description: expect.stringContaining("Quiet period") },
    },
  });
});

it.effect("reuses native fields and decodes defaults", () =>
  Effect.gen(function* () {
    expect(WatchInput.fields.questions).toBe(Generate.DecisionRequest.fields.questions);
    expect(yield* Schema.decodeUnknownEffect(WatchInput)(input)).toEqual({
      ...input,
      input: [],
      timeoutSeconds: 7200,
      debounceMilliseconds: 30_000,
    });
    for (const invalid of [
      { ...input, questions: [] },
      { ...input, questions: [{ type: "unknown" }] },
      { ...input, input: [{ type: "unknown" }] },
      { questions: input.questions },
      ...[0, -1, Infinity, NaN].map((timeoutSeconds) => ({ ...input, timeoutSeconds })),
      ...[-1, Infinity, -Infinity, NaN].map((debounceMilliseconds) => ({
        ...input,
        debounceMilliseconds,
      })),
    ]) {
      expect((yield* Schema.decodeUnknownEffect(WatchInput)(invalid).pipe(Effect.exit))._tag).toBe(
        "Failure",
      );
    }
  }),
);

it.effect(
  "allows zero and finite nonnegative conversation debounce values independently of timeout",
  () =>
    Effect.gen(function* () {
      for (const debounceMilliseconds of [0, 0.5, 750]) {
        const decoded = yield* Schema.decodeUnknownEffect(WatchInput)({
          ...input,
          debounceMilliseconds,
        });
        expect(decoded.debounceMilliseconds).toBe(debounceMilliseconds);
        expect(decoded.timeoutSeconds).toBe(7200);
      }
    }),
);

it.effect("defaults operator workers to four and accepts only positive integers", () =>
  Effect.gen(function* () {
    const model = { providerID: "operator-provider", id: "operator-model" };
    expect((yield* Schema.decodeUnknownEffect(Options)({ model })).workers).toBe(4);
    for (const workers of [1, 2, 6]) {
      expect((yield* Schema.decodeUnknownEffect(Options)({ model, workers })).workers).toBe(
        workers,
      );
    }
    for (const workers of [0, -1, 1.5, Infinity, -Infinity, NaN]) {
      expect(
        (yield* Schema.decodeUnknownEffect(Options)({ model, workers }).pipe(Effect.exit))._tag,
      ).toBe("Failure");
    }
    const watch = yield* Schema.decodeUnknownEffect(WatchInput)({
      ...input,
      workers: 100,
      model: { providerID: "agent-provider", id: "agent-model" },
    });
    expect(watch).not.toHaveProperty("workers");
    expect(watch).not.toHaveProperty("model");
    expect(WatchStatus.members[1].fields).not.toHaveProperty("diagnostic");
  }),
);

it.effect("keeps all verified event fields and distinct root and thread", () =>
  Effect.gen(function* () {
    const event = {
      type: "im.message.receive_v1",
      chat_id: "chat",
      message_id: "message",
      content: "rendered",
      chat_type: "group",
      create_time: "1",
      event_id: "event",
      id: "legacy",
      mentions: [{ id: "person", key: "@_user_1", name: "Person" }],
      message_type: "text",
      reply_to: "parent",
      root_id: "root",
      sender_id: "sender",
      sender_type: "user",
      thread_id: "thread",
      timestamp: "2",
      update_time: "3",
    };
    expect(yield* Schema.decodeUnknownEffect(LarkEvent)(event)).toEqual(event);
    expect(Schema.is(WatchStatus)({ state: "active" })).toBe(false);
    expect(Schema.is(WatchStatus)({ state: "stopped" })).toBe(true);
  }),
);
