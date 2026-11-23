import { DateTime, Deferred, Effect, Stream } from "effect";
import { expect, test, vi } from "vitest";
import {
  Health,
  type EventKind,
  type Event,
  Rejected,
  type IntegrationError,
  Unavailable,
} from "../../src/core/integration.ts";
import { Feeds } from "../../src/core/feeds.ts";
import { runWatch } from "../../src/core/watch.ts";
import { connect } from "../harness.ts";

const pass = { script: "return input.event.data" };

test("watches exposes each watch's public view without its handles", async () => {
  const h = await connect();
  try {
    h.stream({ watchId: "listed", ...pass });
    const listed = () => h.call("watches");
    await vi.waitFor(async () => expect((await listed()).watches).toHaveLength(1));
    const [watch] = (await listed()).watches;
    expect(Object.keys(watch).sort()).toEqual([
      "arguments",
      "event",
      "expiresAt",
      "startedAt",
      "watchId",
    ]);
    expect(watch).toMatchObject({ watchId: "listed", event: "lark.im.message.receive_v1" });
  } finally {
    await h.close();
  }
});

// OpenCode treats -32024 as a permanent rejection and retries -32603 as a transient failure.
const subscribeOnce = async (error: IntegrationError) => {
  const h = await connect({ otherSubscribeError: error });
  try {
    return await h.stream({ watchId: "subscribe", ...pass }, "other.ping").done;
  } finally {
    await h.close();
  }
};

test("a rejected feed answers with the permanent code", async () => {
  expect(
    await subscribeOnce(new Rejected({ message: "The token cannot read that project" })),
  ).toMatchObject({
    code: -32024,
    message: "The token cannot read that project",
  });
});

test("an unavailable feed answers with the transient code", async () => {
  expect(await subscribeOnce(new Unavailable({ message: "GitLab is restarting" }))).toMatchObject({
    code: -32603,
  });
});

test("announces active before delivering the first event of a short stream", async () => {
  const health = Stream.succeed<Health>(Health.Live());
  const events = Stream.fromIterable<Event>([
    { id: "one", occurredAt: DateTime.makeUnsafe(0), payload: { ok: true } },
  ]);
  const event = {
    name: "tick",
    description: "A tick.",
    payload: { type: "object" },
    subscribe: () => Effect.succeed({ events, health }),
  } satisfies EventKind;
  const types: Array<string> = [];
  const reason = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        return yield* runWatch({
          watchId: "short",
          eventKind: event,
          params: {},
          expiresInSeconds: 60,
          eventTimeoutSeconds: 30,
          ended: yield* Deferred.make<string>(),
          script: () => Effect.succeed("delivered"),
          notify: (n) => Effect.sync(() => types.push(n.type)),
        }).pipe(
          Effect.provideService(
            Feeds.Service,
            Feeds.Service.of({ acquire: () => Effect.succeed({ events, health }) }),
          ),
        );
      }),
    ),
  );
  expect(reason).toBe("Events stopped arriving. Arm a new watch");
  expect(types).toEqual(["active", "event", "terminated"]);
});
