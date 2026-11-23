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
import { decodeArguments, inputSchemaDocument, Invalid } from "./arguments.ts";
import { Registry } from "../source/registry.ts";
import { DecisionModel, type Decide } from "../decision/decision-model.ts";
import { bytes, MAX_CODE_BYTES } from "../limits.ts";
import type { EventType, SourceError } from "../source/contract.ts";
import { makeRunCode, makeWatchState } from "../code/code.ts";
import { inputJsonSchema } from "../source/tool.ts";
import { Feeds } from "../watch/feeds.ts";
import { runWatch, type Notification } from "../watch/watch.ts";
import type { Harness, Send, Tools } from "../harness/harness.ts";
import { get, type Name as HarnessName } from "../harness/catalog.ts";

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

/** A decoded and resolved watch that has not claimed its id yet. */
interface Armed {
  readonly id: string;
  readonly event: string;
  readonly watch: Watch;
  /** Claims the id, runs the watch until it ends, and releases the id. */
  readonly run: (options: {
    readonly notify: (notification: Notification) => Effect.Effect<void>;
    readonly onSubscribed?: Effect.Effect<void>;
  }) => Effect.Effect<string, Invalid | SourceError, Scope.Scope>;
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
  const watch = inputSchemaDocument({
    target: {
      description:
        "A link or shorthand that names the resource, required for events whose list_events entry lists target.",
      required: false,
    },
    toolPaths: options.toolPaths,
    defaultExpiresInSeconds: options.defaultExpiresInSeconds,
  });
  return {
    ...(Object.keys(definitions).length === 0 ? {} : { $defs: definitions }),
    ...watch,
    properties: {
      event: { type: "string", description: "The event to watch, from list_events." },
      ...watch.properties,
      ...properties,
    },
    required: ["event", ...watch.required, ...required],
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

  for (const source of options.registry.sources)
    for (const [name, tool] of Object.entries(source.tools))
      server.registerTool(
        `${source.name}_${name}`,
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
        "List open watches with their event, arguments, and expiry, so you can reuse the arguments or find a watch by its id. A watch armed with target also lists the params the target resolved to. A watch that already ended is not listed.",
    },
    () =>
      text({
        watches: [...watches].map(([id, watch]) => ({ id, ...watch.view })),
      }),
  );
  server.registerTool(
    "unwatch",
    {
      description:
        "End a watch early and wait until it has closed. Returns { id, stopped }. stopped is false when no open watch has that id.",
      inputSchema: fromJsonSchema<{ id: string }>({
        type: "object",
        properties: {
          id: {
            type: "string",
            minLength: 1,
            description:
              "The watch to end, as the `watches` tool lists it or as you named it when you armed it.",
          },
        },
        required: ["id"],
        additionalProperties: false,
      }),
    },
    ({ id }) =>
      run(
        Effect.gen(function* () {
          const watch = watches.get(id);
          if (watch) {
            yield* Deferred.succeed(watch.ended, "Stopped");
            yield* Deferred.await(watch.finished);
          }
          return text({ id, stopped: watch !== undefined });
        }),
      ),
  );

  const catalogue = () =>
    options.registry.events.map((event) => ({
      name: event.name,
      description: event.description || event.name,
      delivery: ["push"],
      inputSchema: inputSchemaDocument({
        target:
          event.target === undefined
            ? undefined
            : { description: event.target.description, required: true },
        toolPaths,
        defaultExpiresInSeconds: options.defaultExpiresInSeconds,
      }),
      payloadSchema: { anyOf: [event.payload, { type: "string" }] },
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

  const arm = (input: {
    readonly name: string;
    readonly args: unknown;
    /** What `watches` lists as the arguments, which is enough to arm the watch again. */
    readonly view: Record<string, unknown>;
  }): Effect.Effect<Armed, Invalid | SourceError> =>
    Effect.gen(function* () {
      const event = options.registry.get(input.name);
      if (event === undefined)
        return yield* Effect.fail(
          new Invalid({ message: `Unknown event: ${input.name}; choose a listed event` }),
        );
      const decoded = yield* decodeArguments(input.args);
      if (bytes(decoded.code) > MAX_CODE_BYTES)
        return yield* Effect.fail(
          new Invalid({ message: `code exceeds ${MAX_CODE_BYTES} bytes; shorten it` }),
        );
      const params = yield* resolveParams(event, decoded.target);
      const expiresInSeconds = decoded.expiresInSeconds ?? options.defaultExpiresInSeconds;
      const startedAt = yield* Clock.currentTimeMillis;
      const watch: Watch = {
        view: {
          event: event.name,
          startedAt: new Date(startedAt).toISOString(),
          expiresAt: new Date(startedAt + expiresInSeconds * 1000).toISOString(),
          arguments: input.view,
          ...(decoded.target === undefined ? {} : { params }),
        },
        ended: yield* Deferred.make<string>(),
        finished: yield* Deferred.make<void>(),
      };
      const claim = Effect.acquireRelease(
        Effect.suspend(() => {
          if (watches.has(decoded.id))
            return Effect.fail(
              new Invalid({
                message: "id is already active; choose another id or stop the watch",
              }),
            );
          watches.set(decoded.id, watch);
          return Effect.void;
        }),
        () =>
          Effect.sync(() => watches.delete(decoded.id)).pipe(
            Effect.andThen(Deferred.succeed(watch.finished, undefined)),
          ),
      );
      return {
        id: decoded.id,
        event: event.name,
        watch,
        run: ({ notify, onSubscribed }) =>
          Effect.gen(function* () {
            yield* claim;
            return yield* runWatch({
              id: decoded.id,
              eventType: event,
              params,
              expiresInSeconds,
              eventTimeoutSeconds: options.eventTimeoutSeconds,
              ended: watch.ended,
              onSubscribed,
              runCode: makeRunCode({
                code: decoded.code,
                sources: options.registry.sources,
                state: yield* makeWatchState,
                decide: options.decide,
              }),
              notify,
            });
          }).pipe(Effect.provideService(Feeds.Service, options.feeds)),
      };
    });

  if (options.harness._tag === "Tools") {
    const harness = options.harness;
    const send: Send = (notification) =>
      Effect.promise(() => server.server.notification(notification));
    const sessionKeys = Object.keys(harness.watchParams.fields);
    const ToolInput = Schema.Struct({ event: Schema.String, ...harness.watchParams.fields });
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
          "Arm a watch and return once it is live. Each event the code does not drop reaches this session as the code's result, until the watch expires or you stop it. Read the event's payload from list_events before you write the code.",
        inputSchema: fromJsonSchema<Record<string, unknown>>(
          watchInputSchema({
            watchParams: harness.watchParams,
            toolPaths,
            defaultExpiresInSeconds: options.defaultExpiresInSeconds,
          }),
        ),
      },
      (input) =>
        run(
          Effect.match(
            Effect.gen(function* () {
              const { event: name, ...session } = yield* Schema.decodeUnknownEffect(ToolInput)(
                input,
              ).pipe(Effect.mapError((error) => new Invalid({ message: error.message })));
              if (harness.check !== undefined)
                yield* harness
                  .check(session)
                  .pipe(Effect.mapError((error) => new Invalid({ message: error.message })));
              const args = Object.fromEntries(
                Object.entries(input).filter(
                  ([key]) => key !== "event" && !sessionKeys.includes(key),
                ),
              );
              const armed = yield* arm({ name, args, view: input });
              const wake = (
                text: string,
                onFailure: (error: { readonly message: string }) => Effect.Effect<void>,
              ) =>
                harness
                  .wake(send, { session, id: armed.id, event: armed.event, text })
                  .pipe(Effect.catchTag("Harness.WakeFailed", onFailure));
              const notify = (notification: Notification) => {
                if (notification.type === "event")
                  return wake(notification.data, (error) =>
                    Deferred.succeed(armed.watch.ended, `Wake failed: ${error.message}`).pipe(
                      Effect.asVoid,
                    ),
                  );
                if (notification.type !== "terminated" || notification.reason === "Stopped")
                  return Effect.void;
                return wake(`Watch ${armed.id} ended: ${notification.reason}`, (error) =>
                  Effect.logWarning(`Termination wake failed: ${error.message}`),
                );
              };
              // Succeeds once the feed is subscribed, or with the reason the watch ended first.
              const ready = yield* Deferred.make<string | undefined, Invalid | SourceError>();
              yield* armed.run({ notify, onSubscribed: Deferred.succeed(ready, undefined) }).pipe(
                Effect.onExit((exit) => {
                  const warn =
                    Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                      ? Effect.logWarning("Watch failed").pipe(
                          Effect.annotateLogs({
                            id: armed.id,
                            error: String(Cause.squash(exit.cause)),
                          }),
                        )
                      : Effect.void;
                  return warn.pipe(Effect.andThen(Deferred.done(ready, exit)));
                }),
                Effect.scoped,
                Effect.forkIn(options.scope),
              );
              const ended = yield* Deferred.await(ready);
              const params = armed.watch.view.params;
              return {
                id: armed.id,
                ...(ended === undefined ? { expiresAt: armed.watch.view.expiresAt } : { ended }),
                ...(params === undefined ? {} : { params }),
              };
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
              new Invalid({ message: "Events are live-only; omit cursor or set it to null" }),
            );
          const args = params.arguments ?? {};
          const armed = yield* arm({ name: params.name, args, view: args });
          yield* armed.run({ notify }).pipe(Effect.scoped);
          return {};
        }).pipe(Effect.mapError(protocolError)),
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
