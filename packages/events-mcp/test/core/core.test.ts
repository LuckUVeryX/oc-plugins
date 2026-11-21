import { DateTime, Deferred, Effect, Stream } from "effect";
import { expect, test, vi } from "vitest";
import { Health, SourceError, type EventType, type Occurrence } from "../../src/core/source.ts";
import { runWatch } from "../../src/core/watch.ts";
import { connect } from "../harness.ts";

const pass = {
  prepare: "return input.event.data",
  transform: "return JSON.stringify(input.prepared)",
};

test("each subscription receives an occurrence the other subscription also saw", async () => {
  const h = await connect();
  const events = () => h.notifications.filter((notification) => notification.type === "event");
  const active = () => h.notifications.filter((notification) => notification.type === "active");
  try {
    h.stream({ watchId: "mine", params: { group: "mine" }, ...pass });
    await vi.waitFor(() => expect(active()).toHaveLength(1));
    h.stream({ watchId: "all", params: { group: "all" }, ...pass });
    await vi.waitFor(() => expect(active()).toHaveLength(2));
    h.publish({ eventId: "shared", payload: { message: { chat_id: "oc_shared" } } });
    await vi.waitFor(() => expect(events()).toHaveLength(2));
    expect(events().map((notification) => [notification.id, notification.eventId])).toEqual(
      active().map((notification) => [notification.id, "shared"]),
    );
  } finally {
    await h.close();
  }
});

test("list_watches exposes each watch's public view without its handles", async () => {
  const h = await connect();
  try {
    h.stream({ watchId: "listed", ...pass });
    const listed = () => h.call("list_watches");
    await vi.waitFor(async () => expect((await listed()).watches).toHaveLength(1));
    const [watch] = (await listed()).watches;
    expect(Object.keys(watch).sort()).toEqual([
      "arguments",
      "eventName",
      "expiresAt",
      "startedAt",
      "watchId",
    ]);
    expect(watch).toMatchObject({ watchId: "listed", eventName: "lark.im.message.receive_v1" });
  } finally {
    await h.close();
  }
});

// OpenCode treats -32024 as a permanent rejection and retries -32603 as a transient failure.
const subscribeOnce = async (error: SourceError) => {
  const h = await connect({ otherSubscribeError: error });
  try {
    return await h.stream({ watchId: "subscribe", ...pass }, "other.ping").done;
  } finally {
    await h.close();
  }
};

test("a rejected upstream answers with the permanent code", async () => {
  expect(
    await subscribeOnce(SourceError.Rejected({ message: "The token cannot read that project" })),
  ).toMatchObject({
    code: -32024,
    message: "The token cannot read that project",
  });
});

test("an unavailable upstream answers with the transient code", async () => {
  expect(
    await subscribeOnce(SourceError.Unavailable({ message: "GitLab is restarting" })),
  ).toMatchObject({ code: -32603 });
});

test("announces active before delivering the first event of a short stream", async () => {
  const health = Stream.succeed<Health>(Health.Live());
  const occurrences = Stream.fromIterable<Occurrence>([
    { id: "one", occurredAt: DateTime.makeUnsafe(0), payload: { ok: true } },
  ]);
  const event = {
    name: "tick",
    description: "A tick.",
    params: {},
    payload: { type: "object" },
    subscribe: () => Effect.succeed({ occurrences, health }),
  } satisfies EventType;
  const types: Array<string> = [];
  const reason = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        return yield* runWatch({
          watchId: "short",
          eventType: event,
          params: {},
          filter: [],
          expiresInSeconds: 60,
          eventTimeoutSeconds: 30,
          ended: yield* Deferred.make<string>(),
          upstream: { acquire: () => Effect.succeed({ occurrences, health }) },
          prepare: () => Effect.succeed({}),
          transform: () => Effect.succeed("delivered"),
          notify: (type) => Effect.sync(() => types.push(type)),
        });
      }),
    ),
  );
  expect(reason).toBe("Event source ended");
  expect(types).toEqual(["active", "event", "terminated"]);
});
