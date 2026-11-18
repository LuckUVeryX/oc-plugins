import { Cause, Effect, Exit } from "effect";
import { expect, onTestFinished, test, vi } from "vitest";
import { connect } from "./harness.ts";

const observed = vi.hoisted(() => ({
  entered: Promise.withResolvers<void>(),
  exited: Promise.withResolvers<boolean>(),
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
  const h = await connect();
  onTestFinished(() => h.close());
  const oversized = h.stream({
    prepare: "return input.event.data",
    watchId: "oversized",
    transform: "a".repeat(65537),
  });
  expect(await oversized.done).toMatchObject({ code: -32602 });
  h.stream({
    prepare: "return input.event.data",
    watchId: "raw",
    transform: "return JSON.stringify(input.prepared)",
  });
  h.stream({
    prepare: "return input.event.data",
    watchId: "transformed",
    filter: { "message.chat_id": "match" },
    transform:
      'if (input.event.eventId === "suppressed") return null; if (input.event.eventId === "failed") throw new Error("failure"); return input.event.data.message.chat_id',
  });
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(2),
  );
  const [raw, transformed] = h.notifications.filter((n) => n.type === "active").map((n) => n.id);
  const publish = (eventId: string, chat = "match") =>
    h.publish({ eventId, payload: { message: { chat_id: chat } } });
  await publish("one", "other");
  await publish("two");
  await publish("suppressed");
  await publish("failed");
  await publish("sentinel");
  await vi.waitFor(() =>
    expect(h.notifications.some((n) => n.id === transformed && n.type === "terminated")).toBe(true),
  );
  const events = () => h.notifications.filter((n) => n.type === "event");
  expect(events().filter((n) => n.id === transformed)).toEqual([
    {
      type: "event",
      id: transformed,
      data: "match",
      eventId: "two",
      timestamp: "2026-01-01T00:00:00.000Z",
      origin: "upstream",
    },
  ]);
  expect(
    events()
      .filter((n) => n.id === raw)
      .map((n) => n.eventId),
  ).toEqual(["one", "two", "suppressed", "failed", "sentinel"]);
  expect(stdout).not.toHaveBeenCalled();
  expect(
    h.notifications.find((n) => n.id === transformed && n.type === "terminated")?.reason,
  ).toContain("transform failed on failed:");
  h.stream({ prepare: "return input.event.data", watchId: "slow", transform: "while (true) {}" });
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(3),
  );
  const slow = h.notifications.filter((n) => n.type === "active")[2]?.id;
  await publish("slow-event");
  await observed.entered.promise;
  await h.call("stop_watch", { watchId: "slow" });
  expect(await observed.exited.promise).toBe(true);
  await publish("after-stop");
  await vi.waitFor(() =>
    expect(events().some((n) => n.id === raw && n.eventId === "after-stop")).toBe(true),
  );
  expect(events().filter((n) => n.id === transformed)).toHaveLength(1);
  expect(events().filter((n) => n.id === slow)).toEqual([]);
});
