import { BunServices } from "@effect/platform-bun";
import { Plugin } from "@opencode/plugin/effect";
import { Tool } from "@opencode/schema/tool";
import { Context, Effect, JsonSchema, Layer, Schema } from "effect";
import { larkLayer } from "./lark.ts";
import { Options, openCodeLayer } from "./opencode.ts";
import { Relay, relayLayer, WatchInput, WatchStatus } from "./relay.ts";
import { TransformInput } from "./transform.ts";

export default Plugin.define({
  id: "oc-lark-relay",
  effect: Effect.fn("LarkRelay.setup")(function* (ctx) {
    const options = yield* Schema.decodeUnknownEffect(Options)(ctx.options).pipe(Effect.orDie);
    const inputDocument = Schema.toJsonSchemaDocument(TransformInput);
    const outputDocument = Schema.toJsonSchemaDocument(Schema.NullOr(Schema.String));
    const transformHelp = yield* Schema.decodeUnknownEffect(Schema.Json)({
      inputSchema: {
        $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
        ...inputDocument.schema,
        $defs: inputDocument.definitions,
      },
      outputSchema: {
        $schema: JsonSchema.META_SCHEMA_URI_DRAFT_2020_12,
        ...outputDocument.schema,
        $defs: outputDocument.definitions,
      },
    }).pipe(Effect.orDie);
    const services = Layer.merge(larkLayer, openCodeLayer(ctx, options));
    const runtime = yield* Layer.build(
      relayLayer({ workers: options.workers }).pipe(
        Layer.provide(services),
        Layer.provide(BunServices.layer),
      ),
    ).pipe(Effect.orDie);
    const relay = Context.get(runtime, Relay);
    yield* ctx.tool.transform((tools) => {
      tools.add({
        name: "lark_watch",
        input: WatchInput,
        output: WatchStatus,
        description: `Watch received Lark messages for the calling session using the operator's decision model. Call lark_transform_schema when writing or changing transform code.

Success replaces this session's configuration, cancels old pending work, and renews its timeout. Returns active configuration, listener state, and absolute expiresAt. Preparation failures return a tool error and preserve the current watch and deadline. Batch failures are logged and discarded while workers continue. expiresAt marks when listening stops. Already accepted model requests can still incur cost.`,
        execute: Effect.fn("lark_watch")(function* (configuration, context) {
          const output = yield* relay
            .watch({ sessionID: context.sessionID, configuration })
            .pipe(
              Effect.mapError((error) => new Tool.Error({ message: String(error.cause), error })),
            );
          return { output };
        }),
      });
      tools.add({
        name: "lark_stop",
        input: Schema.Struct({}),
        output: WatchStatus,
        description:
          "Stop monitoring received Lark messages for the calling session. Cancels its pending relay-owned work and returns stopped status, including on repeated calls.",
        execute: Effect.fn("lark_stop")(function* (_, context) {
          return { output: yield* relay.stop({ sessionID: context.sessionID }) };
        }),
      });
      tools.add({
        name: "lark_status",
        input: Schema.Struct({}),
        output: WatchStatus,
        description:
          "Inspect the calling session's Lark subscription. Returns stopped status, or active status with configuration, listener state, and absolute expiry. The expiry stays unchanged.",
        execute: Effect.fn("lark_status")(function* (_, context) {
          return { output: yield* relay.status({ sessionID: context.sessionID }) };
        }),
      });
      tools.add({
        name: "lark_transform_schema",
        input: Schema.Struct({}),
        output: Schema.Json,
        description:
          "Read the generated input and resolved-value output JSON Schemas when writing or changing transform code.",
        execute: Effect.fn("lark_transform_schema")(() =>
          Effect.succeed({ output: transformHelp }),
        ),
      });
    });
  }),
});
