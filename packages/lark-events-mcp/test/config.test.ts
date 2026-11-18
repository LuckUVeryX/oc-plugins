import { ConfigProvider, Effect } from "effect";
import { expect, test } from "vitest";
import { transformConfig, watchMaxSeconds } from "../src/config.ts";

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

test("validates transform limits", async () => {
  const readLimits = (values: Record<string, string>) =>
    Effect.runPromise(
      transformConfig.parse(ConfigProvider.fromEnvRecord(values, { preserveEmptyStrings: true })),
    );
  expect(await readLimits({})).toEqual({
    timeoutMs: 1000,
    sourceBytes: 65536,
    inputBytes: 1048576,
    outputBytes: 16384,
  });
  for (const key of [
    "LARK_TRANSFORM_TIMEOUT_MS",
    "LARK_TRANSFORM_SOURCE_BYTES",
    "LARK_TRANSFORM_INPUT_BYTES",
    "LARK_TRANSFORM_OUTPUT_BYTES",
  ]) {
    for (const value of ["", "0", "-1", "1.5", "abc"]) {
      await expect(readLimits({ [key]: value })).rejects.toThrow(key);
    }
  }
});
