import { Effect, Layer } from "effect";
import { expect, test } from "vitest";
import { LarkAuth } from "../../src/lark/auth.ts";
import { LarkSocket } from "../../src/lark/socket.ts";
import { Lark } from "../../src/lark/integration.ts";
import { makePrepare, makeWatchState } from "../../src/core/prepare.ts";
import { connect, unusedDecide } from "../harness.ts";

const credentialMessage = "No Lark app credentials";

delete process.env["LARK_APP_ID"];
delete process.env["LARK_APP_SECRET"];

const larkSource = await Effect.runPromise(
  Lark.integration.pipe(
    Effect.provide(
      Layer.merge(
        LarkAuth.layer,
        Layer.succeed(
          LarkSocket.Service,
          LarkSocket.Service.of({
            subscribe: () => Effect.die(new Error("The tool test never subscribes")),
          }),
        ),
      ),
    ),
  ),
);

test("a Lark credential failure reaches the MCP tool result", async () => {
  const h = await connect({ integrations: [larkSource] });
  try {
    const result = await h.callRaw("lark_get", { path: "/im/v1/messages" });
    expect(result.isError).toBe(true);
    expect(result.text).toContain(credentialMessage);
  } finally {
    await h.close();
  }
});

test("a Lark credential failure reaches a prepare failure", async () => {
  const prepare = makePrepare({
    integrations: [larkSource],
    state: await Effect.runPromise(makeWatchState),
    decide: unusedDecide,
  });
  const failure = await Effect.runPromise(
    prepare('return await tools.lark.get({ path: "/im/v1/messages" })', {
      eventId: "event",
      timestamp: new Date().toISOString(),
      data: {},
    }).pipe(Effect.flip),
  );
  expect(failure.message).toContain(credentialMessage);
});
