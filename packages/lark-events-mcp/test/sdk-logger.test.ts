import { expect, test, vi } from "vitest";
import { logger } from "../src/sdk-logger.ts";

test("redacts SDK error objects and dynamic strings from stderr", () => {
  const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
  try {
    logger.error("[ws]", {
      name: "AxiosError",
      config: { data: '{"app_secret":"fake-secret"}' },
    });
    logger.debug("[ws]", "receive message, data: fake-message");
    expect(stderr.mock.calls).toEqual([
      ["[sdk error]", "[ws]", "[redacted]"],
      ["[sdk debug]", "[ws]", "[redacted]"],
    ]);
    expect(JSON.stringify(stderr.mock.calls)).not.toContain("fake-secret");
    expect(JSON.stringify(stderr.mock.calls)).not.toContain("fake-message");
  } finally {
    stderr.mockRestore();
  }
});
