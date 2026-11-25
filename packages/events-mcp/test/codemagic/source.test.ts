import { expect, test } from "vitest";
import { Effect, Fiber, Layer, Option, Queue, Stream, SubscriptionRef } from "effect";
import { HttpClient, HttpClientResponse } from "effect/http";
import { TestClock } from "effect/testing";
import { CodemagicClient, makeClient } from "../../src/codemagic/client.ts";
import { type BuildUpdate, CodemagicSocket } from "../../src/codemagic/socket.ts";
import { Codemagic } from "../../src/codemagic/source.ts";
import { Health, Unavailable } from "../../src/source/contract.ts";

const appId = "0000000000000000000000a1";
const buildId = "0000000000000000000000b1";

const update = (id: string, status?: string, app = appId): BuildUpdate => ({
  buildId: id,
  context: { appId: app },
  payload: status === undefined ? {} : { status },
});

const makeEvent = (client: CodemagicClient.Interface) =>
  Effect.gen(function* () {
    const updates = yield* Queue.unbounded<BuildUpdate>();
    const health = yield* SubscriptionRef.make<Health>(Health.Live());
    const source = yield* Codemagic.source.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(CodemagicClient.Service, client),
          Layer.succeed(CodemagicSocket.Service, {
            subscribe: Effect.succeed({
              updates: Stream.fromQueue(updates),
              health: SubscriptionRef.changes(health),
            }),
          }),
        ),
      ),
    );
    const event = source.events[0];
    if (event === undefined) throw new Error("missing event");
    return { event, updates, health };
  });

const statuses = (
  read: CodemagicClient.Interface["read"],
  params: { readonly appId: string } | { readonly buildId: string },
  count: number,
  drive: (feed: {
    readonly send: (...updates: ReadonlyArray<BuildUpdate>) => Effect.Effect<void>;
    readonly setHealth: (health: Health) => Effect.Effect<void>;
  }) => Effect.Effect<void>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const { event, updates, health } = yield* makeEvent({ read });
      const feed = yield* event.subscribe(params);
      const fiber = yield* feed.events.pipe(
        Stream.take(count),
        Stream.runCollect,
        Effect.forkScoped,
      );
      yield* TestClock.adjust("1 millis");
      yield* drive({
        send: (...values) => Queue.offerAll(updates, values).pipe(Effect.asVoid),
        setHealth: (value) =>
          SubscriptionRef.set(health, value).pipe(Effect.andThen(TestClock.adjust("1 millis"))),
      });
      yield* TestClock.adjust("2 minutes");
      const events = yield* Fiber.join(fiber);
      return events.map(({ payload }) => [payload["id"], payload["status"]]);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

const build = (path: string, status: string) =>
  Option.some({ data: { id: path.replace("/builds/", ""), status } });

test("an app watch sends each change of its builds once, and outlives one it cannot read", async () => {
  const read: CodemagicClient.Interface["read"] = (path) =>
    path === "/builds/gone"
      ? Effect.succeed(Option.none())
      : path === "/builds/broken"
        ? Effect.fail(new Unavailable({ message: "Codemagic HTTP 503" }))
        : Effect.succeed(build(path, "finishing"));
  expect(
    await statuses(read, { appId }, 2, ({ send }) =>
      send(
        update("gone", "failed"),
        update("broken", "failed"),
        update(buildId, "failed", "another app"),
        update(buildId),
        update(buildId, "building"),
        update(buildId, "building"),
        update(buildId, "finished"),
      ),
    ),
  ).toEqual([
    [buildId, "building"],
    [buildId, "finished"],
  ]);
});

test("a build watch starts with the current status, then sends its own changes once", async () => {
  expect(
    await statuses(
      (path) => Effect.succeed(build(path, "building")),
      { buildId },
      2,
      ({ send }) =>
        send(update(buildId, "building"), update("other", "failed"), update(buildId, "finished")),
    ),
  ).toEqual([
    [buildId, "building"],
    [buildId, "finished"],
  ]);
});

test("a build watch sends a status it missed while disconnected", async () => {
  let status = "building";
  expect(
    await statuses(
      (path) => Effect.sync(() => build(path, status)),
      { buildId },
      2,
      ({ setHealth }) =>
        Effect.gen(function* () {
          yield* setHealth(Health.Degraded({ reason: "test" }));
          status = "finished";
          yield* setHealth(Health.Live());
        }),
    ),
  ).toEqual([
    [buildId, "building"],
    [buildId, "finished"],
  ]);
});

test("a build Codemagic answers with its HTML page is rejected", async () => {
  const http = HttpClient.make((request) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        new Response("<!doctype html>", { headers: { "content-type": "text/html" } }),
      ),
    ),
  );
  const error = await Effect.runPromise(
    Effect.gen(function* () {
      const { event } = yield* makeEvent(makeClient({ token: Effect.succeed("test"), http }));
      if (event.target === undefined) throw new Error("missing target");
      return yield* event.target
        .resolve(`https://codemagic.io/app/${appId}/build/${buildId}`)
        .pipe(Effect.flip);
    }),
  );
  expect(error.message).toContain("does not exist");
});
