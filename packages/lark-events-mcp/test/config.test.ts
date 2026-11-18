import { ConfigProvider, Effect } from "effect";
import { expect, test } from "vitest";
import { watchMaxSeconds } from "../src/config.ts";

const read = (value?: string) =>
  Effect.runPromise(
    watchMaxSeconds.parse(
      ConfigProvider.fromEnvRecord(value === undefined ? {} : { LARK_WATCH_MAX_SECONDS: value }, {
        preserveEmptyStrings: true,
      }),
    ),
  );

test("reads the operator watch duration", async () => {
  expect(await read()).toBe(3600);
  expect(await read("12")).toBe(12);
  for (const value of ["", "0", "-1", "1.5", "abc"]) {
    await expect(read(value)).rejects.toThrow("LARK_WATCH_MAX_SECONDS");
  }
});
