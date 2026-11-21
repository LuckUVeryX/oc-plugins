import { Effect, Schema } from "effect";
import { expect, test } from "vitest";
import { SourceName, type Source } from "../src/source.ts";
import { makeTool } from "../src/tool.ts";
import { connect } from "./harness.ts";

const Label = Schema.String.annotate({ identifier: "Label" });

const source: Source = {
  name: Schema.decodeUnknownSync(SourceName)("demo"),
  tools: {
    echo: makeTool({
      description: "Echo the label.",
      input: Schema.Struct({ label: Label }),
      run: ({ label }) => Effect.succeed(label),
    }),
  },
  events: [],
};

test("a source tool whose input uses a named schema works over MCP", async () => {
  const h = await connect({ sources: [source] });
  try {
    const result = await h.callRaw("demo_echo", { label: "hi" });
    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("hi");
  } finally {
    await h.close();
  }
});
