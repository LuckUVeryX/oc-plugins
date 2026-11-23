import { Cause, Effect, Exit } from "effect";
import { expect, onTestFinished, test, vi } from "vitest";
import { connect } from "../harness.ts";

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

test("a script drops, rewrites, and fails events independently of raw watches", async () => {
  const stdout = vi.spyOn(console, "log").mockImplementation(() => undefined);
  onTestFinished(() => stdout.mockRestore());
  const h = await connect();
  onTestFinished(() => h.close());
  h.stream({ watchId: "raw", script: "return input.event.data" });
  h.stream({
    watchId: "scripted",
    script:
      'const { eventId, data } = input.event; if (data.message.chat_id !== "match" || eventId === "suppressed") return null; if (eventId === "failed") throw new Error("failure"); return data.message.chat_id',
  });
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(2),
  );
  const [raw, scripted] = h.notifications.filter((n) => n.type === "active").map((n) => n.id);
  const publish = (eventId: string, chat = "match") =>
    h.publish({ eventId, payload: { message: { chat_id: chat } } });
  await publish("one", "other");
  await publish("two");
  await publish("suppressed");
  await publish("failed");
  await publish("sentinel");
  await vi.waitFor(() =>
    expect(h.notifications.some((n) => n.id === scripted && n.type === "terminated")).toBe(true),
  );
  const events = () => h.notifications.filter((n) => n.type === "event");
  expect(events().filter((n) => n.id === scripted)).toEqual([
    {
      type: "event",
      id: scripted,
      data: "match",
      eventId: "two",
      name: "lark.im.message.receive_v1",
      timestamp: expect.any(String),
    },
  ]);
  expect(
    events()
      .filter((n) => n.id === raw)
      .map((n) => n.eventId),
  ).toEqual(["one", "two", "suppressed", "failed", "sentinel"]);
  expect(stdout).not.toHaveBeenCalled();
  expect(
    h.notifications.find((n) => n.id === scripted && n.type === "terminated")?.reason,
  ).toContain("script failed on failed:");
  h.stream({ watchId: "slow", script: "while (true) {}" });
  await vi.waitFor(() =>
    expect(h.notifications.filter((n) => n.type === "active")).toHaveLength(3),
  );
  const slow = h.notifications.filter((n) => n.type === "active")[2]?.id;
  await publish("slow-event");
  await observed.entered.promise;
  await h.call("unwatch", { watchId: "slow" });
  expect(await observed.exited.promise).toBe(true);
  await publish("after-stop");
  await vi.waitFor(() =>
    expect(events().some((n) => n.id === raw && n.eventId === "after-stop")).toBe(true),
  );
  expect(events().filter((n) => n.id === scripted)).toHaveLength(1);
  expect(events().filter((n) => n.id === slow)).toEqual([]);
});
