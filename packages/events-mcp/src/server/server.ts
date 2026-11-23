import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import { ConfigProvider, Effect, Logger, Schema, Scope } from "effect";
import { decodeTarget, Invalid, targetSchemaDocument } from "./arguments.ts";
import { toolResult } from "./result.ts";
import { registerTools, type Watch } from "./tools.ts";
import { Registry } from "../source/registry.ts";
import { DecisionModel, type Decide } from "../decision/decision-model.ts";
import type { EventType, SourceError, Tool } from "../source/contract.ts";
import { decideTool, type RunCode } from "../code/code.ts";
import { inputJsonSchema } from "../source/tool.ts";
import { Feeds } from "../watch/feeds.ts";
import { HEARTBEAT_MS, runWatch, type Notification } from "../watch/watch.ts";
import type { Harness } from "../harness/harness.ts";
import { get, type Name as HarnessName } from "../harness/catalog.ts";

const DEFAULT_WATCH_SECONDS = 3600;
const DEFAULT_STEP_TIMEOUT_SECONDS = 120;

interface ServerOptions {
  readonly harness: Harness;
  readonly config: ConfigProvider.ConfigProvider;
  readonly scope: Scope.Scope;
  readonly registry: Registry.Interface;
  readonly feeds: Feeds.Interface;
  readonly watches: Map<string, Watch>;
  readonly decide: Decide;
  readonly defaultExpiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
}

const streamParams = Schema.toStandardSchemaV1(
  Schema.Struct({
    name: Schema.String,
    arguments: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    cursor: Schema.optionalKey(Schema.Unknown),
  }),
);

// OpenCode treats -32024 as a permanent rejection and retries -32603 as a transient failure.
const protocolError = (error: Invalid | SourceError) => {
  switch (error._tag) {
    case "Arguments.Invalid":
      return new ProtocolError(INVALID_PARAMS, error.message);
    case "Source.Rejected":
      return new ProtocolError(-32024, error.message);
    case "Source.Unavailable":
      return new ProtocolError(-32603, error.message);
    default: {
      const _exhaustive: never = error;
      return _exhaustive;
    }
  }
};

/** Resolves an event and its target without subscribing yet. */
export type Arm = (
  name: string,
  target: string | undefined,
) => Effect.Effect<
  {
    readonly event: string;
    readonly params: Schema.JsonObject;
    readonly run: (
      notify: (notification: Notification) => Effect.Effect<void>,
      until: Effect.Effect<string>,
      runCode: RunCode,
    ) => Effect.Effect<string, SourceError, Scope.Scope>;
  },
  Invalid | SourceError
>;

const buildServer = (options: ServerOptions): McpServer => {
  // Stdout carries MCP messages, so handler logs must go to stderr.
  const run = <A, E>(effect: Effect.Effect<A, E>, runOptions?: Effect.RunOptions) =>
    Effect.runPromise(
      effect.pipe(
        Effect.provideService(Logger.LogToStderr, true),
        Effect.provideService(ConfigProvider.ConfigProvider, options.config),
      ),
      runOptions,
    );
  const toolPaths = options.registry.sources.flatMap((source) =>
    Object.keys(source.tools).map((name) => `tools.${source.name}.${name}`),
  );
  const server = new McpServer(
    { name: "events-mcp", version: "0.0.0" },
    {
      capabilities: options.harness.capabilities,
      instructions: options.harness.instructions(options.registry.sources),
    },
  );

  const registerTool = (name: string, tool: Tool) =>
    server.registerTool(
      name,
      {
        description: tool.description,
        inputSchema: fromJsonSchema({ ...inputJsonSchema(tool), additionalProperties: false }),
      },
      (input) => run(toolResult(tool.run(input))),
    );
  for (const source of options.registry.sources)
    for (const [name, tool] of Object.entries(source.tools))
      registerTool(`${source.name}_${name}`, tool);

  const catalogue = () =>
    options.registry.events.map((event) => ({
      name: event.name,
      description: [event.description || event.name, event.recipe?.[options.harness._tag] ?? []]
        .flat()
        .join("\n\n"),
      delivery: ["push"],
      inputSchema: targetSchemaDocument(
        event.target === undefined
          ? undefined
          : { description: event.target.description, required: true },
      ),
      // OpenCode drops an event that fails this schema, and the source schemas are generated,
      // so they only document the payload.
      payloadSchema: { anyOf: [event.payload, { type: "object" }] },
    }));

  const resolveParams = (event: EventType, target: string | undefined) =>
    Effect.gen(function* () {
      if (event.target === undefined) {
        if (target === undefined) return {};
        return yield* Effect.fail(new Invalid({ message: `${event.name} does not take a target` }));
      }
      if (target === undefined)
        return yield* Effect.fail(
          new Invalid({ message: `${event.name} needs a target. ${event.target.description}` }),
        );
      return yield* event.target
        .resolve(target)
        .pipe(
          Effect.mapError((error) =>
            error._tag === "Source.Rejected" ? new Invalid({ message: error.message }) : error,
          ),
        );
    });

  const arm: Arm = (name, target) =>
    Effect.gen(function* () {
      const event = options.registry.get(name);
      if (event === undefined)
        return yield* Effect.fail(
          new Invalid({ message: `Unknown event: ${name}; choose a listed event` }),
        );
      const params = yield* resolveParams(event, target);
      return {
        event: event.name,
        params,
        run: (notify, until, runCode) =>
          runWatch({
            eventType: event,
            params,
            eventTimeoutSeconds: options.eventTimeoutSeconds,
            until,
            runCode,
            notify,
          }).pipe(Effect.provideService(Feeds.Service, options.feeds)),
      };
    });

  if (options.harness._tag === "Tools") {
    registerTools({
      server,
      harness: options.harness,
      scope: options.scope,
      run,
      arm,
      watches: options.watches,
      sources: options.registry.sources,
      decide: options.decide,
      catalogue,
      toolPaths,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds,
    });
    return server;
  }

  registerTool("decide", decideTool(options.decide));

  server.server.setRequestHandler(
    "events/list",
    { params: Schema.toStandardSchemaV1(Schema.Struct({})) },
    () => ({ events: catalogue() }),
  );

  server.server.setRequestHandler("events/stream", { params: streamParams }, (params, ctx) => {
    const body = (notification: Notification): Record<string, unknown> => {
      switch (notification.type) {
        case "event":
          return {
            eventId: notification.eventId,
            name: notification.name,
            timestamp: notification.timestamp,
            data: notification.data,
          };
        case "error":
        case "terminated":
          return { error: { code: -32000, message: notification.reason } };
        case "active":
          return { heartbeatMs: HEARTBEAT_MS };
        case "heartbeat":
          return {};
        default: {
          const _exhaustive: never = notification;
          return _exhaustive;
        }
      }
    };
    const notify = (notification: Notification) =>
      Effect.promise(() =>
        ctx.mcpReq.notify({
          method: `notifications/events/${notification.type}`,
          params: {
            ...body(notification),
            cursor: null,
            _meta: { [SUBSCRIPTION_ID_META_KEY]: ctx.mcpReq.id },
          },
        }),
      );
    return run(
      Effect.gen(function* () {
        if (params.cursor !== undefined && params.cursor !== null)
          return yield* Effect.fail(
            new Invalid({ message: "Events are live-only; omit cursor or set it to null" }),
          );
        const { target } = yield* decodeTarget(params.arguments ?? {});
        const armed = yield* arm(params.name, target);
        // OpenCode's automation filters the events and cancels the request to end the watch.
        yield* armed
          .run(notify, Effect.never, (event) => Effect.succeed(event.data))
          .pipe(Effect.scoped);
        return {};
      }).pipe(Effect.mapError(protocolError)),
      { signal: ctx.mcpReq.signal },
    );
  });

  return server;
};

export const make = (options: {
  readonly harness: HarnessName;
  /** Owns every watch this connection arms; closing it ends them. */
  readonly scope: Scope.Scope;
  /** Every open watch in the hub, shared by all connections. */
  readonly watches: Map<string, Watch>;
  readonly defaultExpiresInSeconds?: number;
  readonly eventTimeoutSeconds?: number;
}): Effect.Effect<McpServer, never, Registry.Service | Feeds.Service | DecisionModel.Service> =>
  Effect.gen(function* () {
    const registry = yield* Registry.Service;
    const feeds = yield* Feeds.Service;
    const decisionModel = yield* DecisionModel.Service;
    const config = yield* ConfigProvider.ConfigProvider;
    return buildServer({
      harness: get(options.harness),
      config,
      scope: options.scope,
      watches: options.watches,
      registry,
      feeds,
      decide: decisionModel.decide,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds ?? DEFAULT_WATCH_SECONDS,
      eventTimeoutSeconds: options.eventTimeoutSeconds ?? DEFAULT_STEP_TIMEOUT_SECONDS,
    });
  });

export * as Server from "./server.ts";
