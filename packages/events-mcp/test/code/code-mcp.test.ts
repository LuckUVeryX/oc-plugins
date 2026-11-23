import { Cause, Effect, Exit } from "effect";
import { expect, onTestFinished, test, vi } from "vitest";
import { connect } from "../support.ts";

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

test("a watch's code drops, rewrites, and fails events independently of other watches", async () => {
  const stdout = vi.spyOn(console, "log").mockImplementation(() => undefined);
  onTestFinished(() => stdout.mockRestore());
  const h = await connect({ harness: "claude" });
  onTestFinished(() => h.close());
  const wakes = (id: string) =>
    h.channel.filter((wake) => wake.meta?.["watch_id"] === id).map((wake) => wake.content);
  await h.watch("raw", "return input.event.eventId");
  await h.watch(
    "filtered",
    'const { eventId, data } = input.event; if (data.message.chat_id !== "match" || eventId === "suppressed") return null; if (eventId === "failed") throw new Error("failure"); return data.message.chat_id',
  );
  const publish = (eventId: string, chat = "match") =>
    h.publish({ eventId, payload: { message: { chat_id: chat } } });
  publish("one", "other");
  publish("two");
  publish("suppressed");
  publish("failed");
  publish("sentinel");
  await vi.waitFor(() => expect(wakes("filtered")).toHaveLength(2));
  expect(wakes("filtered")).toEqual(["match", expect.stringContaining("code failed on failed:")]);
  await vi.waitFor(() =>
    expect(wakes("raw")).toEqual(["one", "two", "suppressed", "failed", "sentinel"]),
  );
  expect(stdout).not.toHaveBeenCalled();
  await h.watch("slow", "while (true) {}");
  publish("slow-event");
  await observed.entered.promise;
  await h.call("unwatch", { id: "slow" });
  expect(await observed.exited.promise).toBe(true);
  publish("after-stop");
  await vi.waitFor(() => expect(wakes("raw")).toContain("after-stop"));
  expect(wakes("filtered")).toHaveLength(2);
  expect(wakes("slow")).toEqual([]);
});
