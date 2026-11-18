import { Client } from "@modelcontextprotocol/client";
import { InMemoryTransport, SUBSCRIPTION_ID_META_KEY } from "@modelcontextprotocol/server";
import { Cause, Effect, Exit, PubSub, Schema } from "effect";
import { expect, onTestFinished, test, vi } from "vitest";
import { catalogLayer, LarkCatalog } from "../src/catalog.ts";
import type { SourceMessage } from "../src/producers.ts";
import { makeServer } from "../src/server.ts";

const observed = vi.hoisted(() => ({
  entered: Promise.withResolvers<void>(),
  exited: Promise.withResolvers<boolean>(),
  executions: 0,
}));

vi.mock("@opencode/codemode", async (importOriginal) => {
  const original = await importOriginal<typeof import("@opencode/codemode")>();
  return {
    ...original,
    CodeMode: {
      ...original.CodeMode,
      make: (...args: Parameters<typeof original.CodeMode.make>) => {
        const runtime = original.CodeMode.make(...args);
        return {
          ...runtime,
          execute: (...input: Parameters<typeof runtime.execute>) =>
            Effect.suspend(() => {
              observed.executions++;
              if (input[0] === "while (true) {}") observed.entered.resolve();
              return runtime.execute(...input).pipe(
                Effect.onExit((exit) =>
                  Effect.sync(() => {
                    if (input[0] === "while (true) {}")
                      observed.exited.resolve(
                        Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause),
                      );
                  }),
                ),
              );
            }),
        };
      },
    },
  };
});

test("transforms filtered events independently of raw watches and stops delivery", async () => {
  const stdout = vi.spyOn(console, "log").mockImplementation(() => undefined);
  onTestFinished(() => stdout.mockRestore());
  const messages = await Effect.runPromise(PubSub.unbounded<SourceMessage>());
  const catalog = await Effect.runPromise(
    Effect.gen(function* () {
      return yield* LarkCatalog;
    }).pipe(Effect.provide(catalogLayer)),
  );
  const server = makeServer({
    catalog,
    maxSeconds: 60,
    producers: {
      acquire: () =>
        Effect.succeed({ messages, ready: Effect.void, terminal: Effect.succeed(undefined) }),
    },
  });
  const client = new Client({ name: "transform-test", version: "1" });
  const active: Array<string | number> = [];
  const events: Array<{
    id: string | number;
    data: unknown;
    eventId: string;
    timestamp: string;
    origin: string;
  }> = [];
  const meta = Schema.Struct({
    [SUBSCRIPTION_ID_META_KEY]: Schema.Union([Schema.String, Schema.Number]),
    "lark-events-mcp/timestampOrigin": Schema.optionalKey(Schema.String),
  });
  client.setNotificationHandler(
    "notifications/events/active",
    {
      params: Schema.toStandardSchemaV1(Schema.Struct({ _meta: meta })),
    },
    (value) => {
      active.push(value._meta[SUBSCRIPTION_ID_META_KEY]);
    },
  );
  client.setNotificationHandler(
    "notifications/events/event",
    {
      params: Schema.toStandardSchemaV1(
        Schema.Struct({
          _meta: meta,
          eventId: Schema.String,
          timestamp: Schema.String,
          data: Schema.Unknown,
        }),
      ),
    },
    (value) => {
      events.push({
        id: value._meta[SUBSCRIPTION_ID_META_KEY],
        data: value.data,
        eventId: value.eventId,
        timestamp: value.timestamp,
        origin: value._meta["lark-events-mcp/timestampOrigin"] ?? "",
      });
    },
  );
  const [a, b] = InMemoryTransport.createLinkedPair();
  const controllers: AbortController[] = [];
  const requests: Promise<unknown>[] = [];
  try {
    await server.connect(b);
    await client.connect(a);
    await expect(
      client.request(
        {
          method: "events/stream",
          params: {
            name: "im.message.receive_v1",
            arguments: { watchId: "oversized", transform: "a".repeat(65537) },
          },
        },
        Schema.toStandardSchemaV1(Schema.Unknown),
      ),
    ).rejects.toMatchObject({ code: -32602 });
    for (const args of [
      { watchId: "raw" },
      {
        watchId: "transformed",
        filter: { "message.chat_id": "match" },
        transform:
          'if (input.event.eventId === "suppressed") return null; if (input.event.eventId === "failed") throw new Error("failure"); return input.event.data.message.chat_id',
      },
    ]) {
      const controller = new AbortController();
      controllers.push(controller);
      requests.push(
        client
          .request(
            {
              method: "events/stream",
              params: { name: "im.message.receive_v1", arguments: args },
            },
            Schema.toStandardSchemaV1(Schema.Unknown),
            { signal: controller.signal },
          )
          .catch(() => undefined),
      );
    }
    await vi.waitFor(() => expect(active).toHaveLength(2));
    for (const [id, chat] of [
      ["one", "other"],
      ["two", "match"],
    ] as const) {
      await Effect.runPromise(
        PubSub.publish(messages, {
          kind: "event",
          eventId: id,
          timestamp: "2026-01-01T00:00:00.000Z",
          timestampOrigin: "upstream",
          payload: { message: { chat_id: chat } },
        }),
      );
    }
    await vi.waitFor(() => expect(events).toHaveLength(3));
    expect(observed.executions).toBe(1);
    expect(events.filter((event) => event.id === active[1])).toEqual([
      {
        id: active[1],
        data: "match",
        eventId: "two",
        timestamp: "2026-01-01T00:00:00.000Z",
        origin: "upstream",
      },
    ]);
    for (const id of ["suppressed", "failed", "sentinel"]) {
      await Effect.runPromise(
        PubSub.publish(messages, {
          kind: "event",
          eventId: id,
          timestamp: "2026-01-01T00:00:00.000Z",
          timestampOrigin: "upstream",
          payload: { message: { chat_id: "match" } },
        }),
      );
    }
    await vi.waitFor(() =>
      expect(events.some((event) => event.id === active[1] && event.eventId === "sentinel")).toBe(
        true,
      ),
    );
    expect(observed.executions).toBe(4);
    expect(stdout).not.toHaveBeenCalled();
    expect(events.filter((event) => event.id === active[1]).map((event) => event.eventId)).toEqual([
      "two",
      "sentinel",
    ]);
    await client.request(
      {
        method: "tools/call",
        params: { name: "stop_watch", arguments: { watchId: "transformed" } },
      },
      Schema.toStandardSchemaV1(Schema.Unknown),
    );
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        eventId: "three",
        timestamp: "2026-01-01T00:00:00.000Z",
        timestampOrigin: "upstream",
        payload: { message: { chat_id: "match" } },
      }),
    );
    await vi.waitFor(() => expect(events).toHaveLength(8));
    expect(events.filter((event) => event.id === active[1])).toHaveLength(2);
    const controller = new AbortController();
    controllers.push(controller);
    requests.push(
      client
        .request(
          {
            method: "events/stream",
            params: {
              name: "im.message.receive_v1",
              arguments: { watchId: "slow", transform: "while (true) {}" },
            },
          },
          Schema.toStandardSchemaV1(Schema.Unknown),
          { signal: controller.signal },
        )
        .catch(() => undefined),
    );
    await vi.waitFor(() => expect(active).toHaveLength(3));
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        eventId: "slow-event",
        timestamp: "2026-01-01T00:00:00.000Z",
        timestampOrigin: "upstream",
        payload: { message: { chat_id: "match" } },
      }),
    );
    await observed.entered.promise;
    await client.request(
      { method: "tools/call", params: { name: "stop_watch", arguments: { watchId: "slow" } } },
      Schema.toStandardSchemaV1(Schema.Unknown),
    );
    expect(await observed.exited.promise).toBe(true);
    await Effect.runPromise(
      PubSub.publish(messages, {
        kind: "event",
        eventId: "after-slow-stop",
        timestamp: "2026-01-01T00:00:00.000Z",
        timestampOrigin: "upstream",
        payload: { message: { chat_id: "match" } },
      }),
    );
    await vi.waitFor(() =>
      expect(
        events.some((event) => event.id === active[0] && event.eventId === "after-slow-stop"),
      ).toBe(true),
    );
    expect(events.filter((event) => event.id === active[2])).toEqual([]);
  } finally {
    for (const controller of controllers) controller.abort();
    await Promise.all(requests);
    await client.close();
    await server.close();
    await Effect.runPromise(PubSub.shutdown(messages));
  }
});
