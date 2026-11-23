import { Effect, Schema } from "effect";
import { expect, test } from "vitest";
import { IntegrationName, type Integration } from "../../src/integration/contract.ts";
import { makeTool } from "../../src/integration/tool.ts";
import { connect } from "../harness.ts";

const Label = Schema.String.annotate({ identifier: "Label" });

const integration: Integration = {
  name: Schema.decodeUnknownSync(IntegrationName)("demo"),
  tools: {
    echo: makeTool({
      description: "Echo the label.",
      input: Schema.Struct({ label: Label }),
      run: ({ label }) => Effect.succeed(label),
    }),
  },
  events: [],
};

test("an integration tool whose input uses a named schema works over MCP", async () => {
  const h = await connect({ integrations: [integration] });
  try {
    const result = await h.callRaw("demo_echo", { label: "hi" });
    expect(result.isError).toBeFalsy();
    expect(result.text).toContain("hi");
  } finally {
    await h.close();
  }
});
