import {
  fromJsonSchema,
  INVALID_PARAMS,
  ProtocolError,
  SUBSCRIPTION_ID_META_KEY,
  McpServer,
} from "@modelcontextprotocol/server";
import {
  Cause,
  Clock,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Logger,
  Schema,
  Scope,
} from "effect";
import { isObject } from "effect/Predicate";
import {
  decodeArguments,
  expiresInSecondsProperty,
  inputSchemaDocument,
  Invalid,
  scriptProperty,
  watchIdProperty,
  WatchArguments,
} from "./arguments.ts";
import { Registry } from "./registry.ts";
import { DecisionModel, type Decide } from "./decision-model.ts";
import { bytes, MAX_SCRIPT_BYTES } from "./limits.ts";
import type { EventKind, IntegrationError } from "./integration.ts";
import { makeRunScript, makeWatchState } from "./script.ts";
import { inputJsonSchema } from "./tool.ts";
import { Feeds } from "./feeds.ts";
import { runWatch, type Notification } from "./watch.ts";
import type { Harness, Send, Tools } from "./harness.ts";
import { get, type Name as HarnessName } from "../harness/index.ts";

const DEFAULT_WATCH_SECONDS = 3600;
const DEFAULT_STEP_TIMEOUT_SECONDS = 120;

interface ServerOptions {
  readonly harness: Harness;
  readonly config: ConfigProvider.ConfigProvider;
  readonly scope: Scope.Scope;
  readonly registry: Registry.Interface;
  readonly feeds: Feeds.Interface;
  readonly decide: Decide;
  readonly defaultExpiresInSeconds: number;
  readonly eventTimeoutSeconds: number;
}

type DecodedArguments = Effect.Success<ReturnType<typeof decodeArguments>>;

const streamParams = Schema.toStandardSchemaV1(
  Schema.Struct({
    name: Schema.String,
    arguments: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    cursor: Schema.optionalKey(Schema.Unknown),
  }),
);

const invalidParams = (message: string) => new ProtocolError(INVALID_PARAMS, message);

const text = (value: unknown) => ({
  content: [
    { type: "text" as const, text: typeof value === "string" ? value : JSON.stringify(value) },
  ],
});

interface Watch {
  readonly view: {
    readonly event: string;
    readonly startedAt: string;
    readonly expiresAt: string;
    readonly arguments: Record<string, unknown>;
    readonly params?: Schema.JsonObject;
  };
  readonly ended: Deferred.Deferred<string>;
  readonly finished: Deferred.Deferred<void>;
}

const watchInputSchema = (options: {
  readonly watchParams: Tools["watchParams"];
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}) => {
  const { schema, definitions } = Schema.toJsonSchemaDocument(options.watchParams);
  const properties = isObject(schema["properties"]) ? schema["properties"] : {};
  const required = Array.isArray(schema["required"])
    ? schema["required"].filter((name) => typeof name === "string")
    : [];
  return {
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
    type: "object",
    properties: {
      event: {
        type: "string",
        description: "The event to watch, from list_events.",
      },
      watchId: watchIdProperty,
      target: {
        type: "string",
        minLength: 1,
        description:
          "A link or shorthand that names the resource, required for events whose list_events entry lists target.",
      },
      script: scriptProperty(options.toolPaths),
      expiresInSeconds: expiresInSecondsProperty(options.defaultExpiresInSeconds),
      ...properties,
    },
    required: ["event", "watchId", "script", ...required],
    additionalProperties: false,
  };
};

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
  const watches = new Map<string, Watch>();
  const toolPaths = options.registry.integrations.flatMap((integration) =>
    Object.keys(integration.tools).map((name) => `tools.${integration.name}.${name}`),
  );
  const server = new McpServer(
    { name: "events-mcp", version: "0.0.0" },
    {
      capabilities: options.harness.capabilities,
      instructions: options.harness.instructions(options.registry.integrations),
    },
  );

  for (const integration of options.registry.integrations)
    for (const [name, tool] of Object.entries(integration.tools))
      server.registerTool(
        `${integration.name}_${name}`,
        {
          description: tool.description,
          inputSchema: fromJsonSchema({
            ...inputJsonSchema(tool),
            additionalProperties: false,
          }),
        },
        (input) =>
          run(
            Effect.match(tool.run(input), {
              onFailure: (error) => ({ ...text(error.message), isError: true }),
              onSuccess: text,
            }),
          ),
      );

  server.registerTool(
    "watches",
    {
      description:
        "List open watches with their event, arguments, and expiry, so you can reuse the arguments or find a watchId. A watch armed with target also lists the params the target resolved to. A watch that already ended is not listed.",
    },
    () =>
      text({
        watches: [...watches].map(([watchId, watch]) => ({ watchId, ...watch.view })),
      }),
  );
  server.registerTool(
    "unwatch",
    {
      description:
        "End a watch early and wait until it has closed. Returns { watchId, stopped }. stopped is false when no open watch has that id.",
      inputSchema: fromJsonSchema<{ watchId: string }>({
        type: "object",
        properties: {
          watchId: {
            type: "string",
            minLength: 1,
            description:
              "The watch to end, as the `watches` tool lists it or as you named it when you armed it.",
          },
        },
        required: ["watchId"],
        additionalProperties: false,
      }),
    },
    ({ watchId }) =>
      run(
        Effect.gen(function* () {
          const watch = watches.get(watchId);
          if (watch) {
            yield* Deferred.succeed(watch.ended, "Stopped");
            yield* Deferred.await(watch.finished);
          }
          return text({ watchId, stopped: watch !== undefined });
        }),
      ),
  );

  const catalogue = () =>
    options.registry.events.map((event) => ({
      name: event.name,
      description: event.description || event.name,
      delivery: ["push"],
      inputSchema: inputSchemaDocument({
        target: event.target?.description,
        toolPaths,
        defaultExpiresInSeconds: options.defaultExpiresInSeconds,
      }),
      payloadSchema: { anyOf: [event.payload, { type: "string" }] },
    }));

  const resolveParams = (event: EventKind, decoded: DecodedArguments) =>
    Effect.gen(function* () {
      if (event.target === undefined) {
        if (decoded.target === undefined) return {};
        return yield* Effect.fail(new Invalid({ message: `${event.name} does not take a target` }));
      }
      if (decoded.target === undefined)
        return yield* Effect.fail(
          new Invalid({ message: `${event.name} needs a target. ${event.target.description}` }),
        );
      return yield* event.target
        .resolve(decoded.target)
        .pipe(
          Effect.mapError((error) =>
            error._tag === "Integration.Rejected" ? new Invalid({ message: error.message }) : error,
          ),
        );
    });

  const resolveWatch = (name: string, value: unknown) =>
    Effect.gen(function* () {
      const event = options.registry.get(name);
      if (!event)
        return yield* Effect.fail(
          new Invalid({ message: `Unknown event: ${name}; choose a listed event` }),
        );
      const decoded = yield* decodeArguments(value);
      if (bytes(decoded.script) > MAX_SCRIPT_BYTES)
        return yield* Effect.fail(
          new Invalid({ message: `script exceeds ${MAX_SCRIPT_BYTES} bytes; shorten it` }),
        );
      const eventParams = yield* resolveParams(event, decoded);
      return { event, decoded, eventParams };
    });

  const newWatch = (watch: {
    readonly event: string;
    readonly arguments: Record<string, unknown>;
    readonly params: Schema.JsonObject | undefined;
    readonly expiresInSeconds: number;
  }) =>
    Effect.gen(function* () {
      const startedAt = yield* Clock.currentTimeMillis;
      const ended = yield* Deferred.make<string>();
      const finished = yield* Deferred.make<void>();
      return {
        view: {
          event: watch.event,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + watch.expiresInSeconds * 1000).toISOString(),
          arguments: watch.arguments,
          ...(watch.params === undefined ? {} : { params: watch.params }),
        },
        ended,
        finished,
      } satisfies Watch;
    });

  const release = (watchId: string, watch: Watch) =>
    Effect.gen(function* () {
      watches.delete(watchId);
      yield* Deferred.succeed(watch.finished, undefined);
    });

  const finish = (watchId: string, watch: Watch) => Effect.ensuring(release(watchId, watch));

  const startWatch = (input: {
    readonly event: EventKind;
    readonly decoded: DecodedArguments;
    readonly eventParams: Schema.JsonObject;
    readonly watch: Watch;
    readonly notify: (notification: Notification) => Effect.Effect<void>;
    readonly ready?: Deferred.Deferred<string | undefined, IntegrationError> | undefined;
  }) =>
    Effect.gen(function* () {
      const script = makeRunScript({
        code: input.decoded.script,
        integrations: options.registry.integrations,
        state: yield* makeWatchState,
        decide: options.decide,
      });
      return yield* runWatch({
        watchId: input.decoded.watchId,
        eventKind: input.event,
        params: input.eventParams,
        expiresInSeconds: input.decoded.expiresInSeconds ?? options.defaultExpiresInSeconds,
        eventTimeoutSeconds: options.eventTimeoutSeconds,
        ended: input.watch.ended,
        ready: input.ready,
        script,
        notify: input.notify,
      }).pipe(Effect.provideService(Feeds.Service, options.feeds));
    });

  if (options.harness._tag === "Tools") {
    const harness = options.harness;
    const send: Send = (notification) =>
      Effect.promise(() => server.server.notification(notification));
    const WatchInput = Schema.Struct({
      event: Schema.String,
      ...WatchArguments.fields,
      ...harness.watchParams.fields,
    });
    const watchTool = watchInputSchema({
      watchParams: harness.watchParams,
      toolPaths,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds,
    });
    server.registerTool(
      "list_events",
      {
        description:
          "List the events you can watch, with each event's arguments and payload. Read it before you arm a watch.",
      },
      () => text({ events: catalogue() }),
    );
    server.registerTool(
      "watch",
      {
        description:
          "Arm a watch and return once it is live. Each event the script does not drop reaches this session as the script's result, until the watch expires or you stop it. Read the event's payload from list_events before you write the script.",
        inputSchema: fromJsonSchema<typeof WatchInput.Type>(watchTool),
      },
      (input) =>
        run(
          Effect.match(
            Effect.gen(function* () {
              const {
                event: name,
                watchId,
                target,
                script,
                expiresInSeconds: requestedExpiresInSeconds,
                ...session
              } = yield* Schema.decodeUnknownEffect(WatchInput)(input).pipe(
                Effect.mapError((error) => new Invalid({ message: error.message })),
              );
              if (harness.check !== undefined)
                yield* harness
                  .check(session)
                  .pipe(Effect.mapError((error) => new Invalid({ message: error.message })));
              const { event, decoded, eventParams } = yield* resolveWatch(name, {
                watchId,
                ...(target === undefined ? {} : { target }),
                script,
                ...(requestedExpiresInSeconds === undefined
                  ? {}
                  : { expiresInSeconds: requestedExpiresInSeconds }),
              });
              const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
              const watch = yield* newWatch({
                event: event.name,
                arguments: input,
                params: target === undefined ? undefined : eventParams,
                expiresInSeconds,
              });
              const ready = yield* Deferred.make<string | undefined, IntegrationError>();
              const claimed = yield* Effect.sync(() => {
                if (watches.has(decoded.watchId)) return false;
                watches.set(decoded.watchId, watch);
                return true;
              });
              if (!claimed)
                return yield* Effect.fail(
                  new Invalid({
                    message: "watchId is already active; choose another ID or stop the watch",
                  }),
                );
              const wake = (
                text: string,
                onFailure: (error: { readonly message: string }) => Effect.Effect<void>,
              ) =>
                harness
                  .wake(send, {
                    session,
                    watchId: decoded.watchId,
                    event: event.name,
                    text,
                  })
                  .pipe(Effect.catchTag("Harness.WakeFailed", onFailure));
              yield* startWatch({
                event,
                decoded,
                eventParams,
                watch,
                ready,
                notify: (notification) => {
                  if (notification.type === "event")
                    return wake(notification.data, (error) =>
                      Deferred.succeed(watch.ended, `Wake failed: ${error.message}`).pipe(
                        Effect.as(undefined),
                      ),
                    );
                  if (notification.type !== "terminated") return Effect.void;
                  if (notification.reason === "Stopped") return Effect.void;
                  return wake(`Watch ${decoded.watchId} ended: ${notification.reason}`, (error) =>
                    Effect.logWarning(`Termination wake failed: ${error.message}`),
                  );
                },
              }).pipe(
                Effect.onExit((exit) => {
                  const warn =
                    Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                      ? Effect.logWarning("Watch failed").pipe(
                          Effect.annotateLogs({
                            watchId: decoded.watchId,
                            error: String(Cause.squash(exit.cause)),
                          }),
                        )
                      : Effect.void;
                  return warn.pipe(Effect.andThen(Deferred.done(ready, exit)));
                }),
                finish(decoded.watchId, watch),
                Effect.scoped,
                Effect.forkIn(options.scope),
              );
              const outcome = yield* Deferred.await(ready).pipe(
                Effect.mapError((error) => new Invalid({ message: error.message })),
              );
              const result =
                outcome === undefined
                  ? { watchId: decoded.watchId, expiresAt: watch.view.expiresAt }
                  : { watchId: decoded.watchId, ended: outcome };
              return target === undefined ? result : { ...result, params: eventParams };
            }),
            {
              onFailure: (error) => ({ ...text(error.message), isError: true }),
              onSuccess: text,
            },
          ),
        ),
    );
  } else {
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
              invalidParams("Events are live-only; omit cursor or set it to null"),
            );
          const { event, decoded, eventParams } = yield* resolveWatch(
            params.name,
            params.arguments ?? {},
          ).pipe(
            Effect.mapError((error) =>
              error._tag === "Arguments.Invalid"
                ? invalidParams(error.message)
                : new ProtocolError(
                    error._tag === "Integration.Unavailable" ? -32603 : -32024,
                    error.message,
                  ),
            ),
          );
          const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
          const watch = yield* newWatch({
            event: params.name,
            arguments: params.arguments ?? {},
            params: decoded.target === undefined ? undefined : eventParams,
            expiresInSeconds,
          });
          yield* Effect.acquireRelease(
            Effect.gen(function* () {
              if (watches.has(decoded.watchId))
                return yield* Effect.fail(
                  invalidParams("watchId is already active; choose another ID or stop the watch"),
                );
              watches.set(decoded.watchId, watch);
            }),
            () => release(decoded.watchId, watch),
          );
          yield* startWatch({ event, decoded, eventParams, watch, notify }).pipe(
            Effect.mapError((error: IntegrationError) => {
              const code = error._tag === "Integration.Rejected" ? -32024 : -32603;
              return new ProtocolError(code, error.message);
            }),
          );
          return {};
        }).pipe(Effect.scoped),
        { signal: ctx.mcpReq.signal },
      );
    });
  }

  return server;
};

export const make = (options: {
  readonly harness: HarnessName;
  /** Owns every watch this connection arms; closing it ends them. */
  readonly scope: Scope.Scope;
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
      registry,
      feeds,
      decide: decisionModel.decide,
      defaultExpiresInSeconds: options.defaultExpiresInSeconds ?? DEFAULT_WATCH_SECONDS,
      eventTimeoutSeconds: options.eventTimeoutSeconds ?? DEFAULT_STEP_TIMEOUT_SECONDS,
    });
  });

export * as Server from "./server.ts";
