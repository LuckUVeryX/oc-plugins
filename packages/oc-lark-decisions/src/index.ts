import { BunServices } from "@effect/platform-bun";
import { Plugin } from "@opencode/plugin/effect";
import { Tool } from "@opencode/schema/tool";
import { Context, Effect, JsonSchema, Layer, Schema } from "effect";
import { larkLayer } from "./lark.ts";
import { Options, openCodeLayer } from "./opencode.ts";
import { Relay, relayLayer, WatchInput, WatchStatus } from "./relay.ts";
import { TransformInput } from "./transform.ts";

export default Plugin.define({
  id: "oc-lark-decisions",
  effect: Effect.fn("LarkDecisions.setup")(function* (ctx) {
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
      relayLayer(options).pipe(Layer.provide(services), Layer.provide(BunServices.layer)),
    ).pipe(Effect.orDie);
    const relay = Context.get(runtime, Relay);
    yield* ctx.tool.transform((tools) => {
      tools.add({
        name: "lark_watch",
        input: WatchInput,
        output: WatchStatus,
        description: `Watch bot-received Lark conversations, evaluate decisions, and queue transformed prompts in this session. Does not send Lark messages.

Each batch combines input with events and up to five recent messages from the triggering thread, or only the standalone message itself. Questions evaluate independently using the operator's model. Call lark_transform_schema before writing or changing transforms.

Success replaces this session's watch and renews expiry; preparation failure preserves it. Failed batches are discarded. Source exit stops listening without reconnecting. Replacement and expiry leave queued prompts intact. Model calls may incur cost.`,
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
          "Stop this session's watch and cancel pending relay work. Safe to repeat; already queued prompts remain.",
        execute: Effect.fn("lark_stop")(function* (_, context) {
          return { output: yield* relay.stop({ sessionID: context.sessionID }) };
        }),
      });
      tools.add({
        name: "lark_status",
        input: Schema.Struct({}),
        output: WatchStatus,
        description:
          "Read this session's watch configuration, listener state, and expiry without renewing it.",
        execute: Effect.fn("lark_status")(function* (_, context) {
          return { output: yield* relay.status({ sessionID: context.sessionID }) };
        }),
      });
      tools.add({
        name: "lark_transform_schema",
        input: Schema.Struct({}),
        output: Schema.Json,
        description:
          "Read the transform input and resolved-output JSON Schemas before writing or changing transform code.",
        execute: Effect.fn("lark_transform_schema")(() =>
          Effect.succeed({ output: transformHelp }),
        ),
      });
    });
  }),
});
