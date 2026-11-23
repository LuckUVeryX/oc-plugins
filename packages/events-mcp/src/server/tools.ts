// Claude Code and Codex have no MCP events, so they arm watches through tools and the
// server wakes the session itself. Delete this file once they support MCP events.
import { fromJsonSchema, type McpServer } from "@modelcontextprotocol/server";
import { Cause, Clock, Deferred, Effect, Exit, Schema, type Scope } from "effect";
import { isObject } from "effect/Predicate";
import { decodeWatch, Invalid, targetSchemaDocument, watchSchemaDocument } from "./arguments.ts";
import { text, toolResult } from "./result.ts";
import type { Arm } from "./server.ts";
import type { Source, SourceError } from "../source/contract.ts";
import type { Decide } from "../decision/decision-model.ts";
import { makeRunCode, makeWatchState } from "../code/code.ts";
import { bytes, MAX_CODE_BYTES } from "../limits.ts";
import type { Notification } from "../watch/watch.ts";
import type { Send, Tools } from "../harness/harness.ts";

export interface Watch {
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

const wakeText = (data: Schema.Json) => (typeof data === "string" ? data : JSON.stringify(data));

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
  const watch = watchSchemaDocument(
    targetSchemaDocument({
      description:
        "A link or shorthand that names the resource, required for events whose list_events entry lists target.",
      required: false,
    }),
    options,
  );
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

export const registerTools = (options: {
  readonly server: McpServer;
  readonly harness: Tools;
  /** Owns every watch this connection arms; closing it ends them. */
  readonly scope: Scope.Scope;
  readonly run: <A, E>(effect: Effect.Effect<A, E>) => Promise<A>;
  readonly arm: Arm;
  /** Every open watch in the hub, shared by all connections. */
  readonly watches: Map<string, Watch>;
  readonly sources: ReadonlyArray<Source>;
  readonly decide: Decide;
  readonly catalogue: () => ReadonlyArray<unknown>;
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}) => {
  const { server, harness, run, arm, watches } = options;
  const send: Send = (notification) =>
    Effect.promise(() => server.server.notification(notification));
  const sessionKeys = Object.keys(harness.watchParams.fields);
  const ToolInput = Schema.Struct({ event: Schema.String, ...harness.watchParams.fields });

  server.registerTool(
    "watches",
    {
      description:
        "List every open watch from every session with its event, arguments, and expiry, so you can reuse the arguments or find a watch by its id. A watch armed with target also lists the params the target resolved to. A watch that already ended is not listed.",
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

  server.registerTool(
    "list_events",
    {
      description:
        "List the events you can watch, with each event's arguments and payload. Read it before you arm a watch.",
    },
    () => text({ events: options.catalogue() }),
  );
  server.registerTool(
    "watch",
    {
      description:
        "Arm a watch and return once it is live. Each event the code does not drop reaches this session as the code's result, until the watch expires or you stop it. Read the event's payload from list_events before you write the code.",
      inputSchema: fromJsonSchema<Record<string, unknown>>(
        watchInputSchema({
          watchParams: harness.watchParams,
          toolPaths: options.toolPaths,
          defaultExpiresInSeconds: options.defaultExpiresInSeconds,
        }),
      ),
    },
    (input) =>
      run(
        toolResult(
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
            const {
              id,
              expiresInSeconds = options.defaultExpiresInSeconds,
              target,
              code,
            } = yield* decodeWatch(args);
            if (bytes(code) > MAX_CODE_BYTES)
              return yield* Effect.fail(
                new Invalid({ message: `code exceeds ${MAX_CODE_BYTES} bytes; shorten it` }),
              );
            const armed = yield* arm(name, target);
            const runCode = makeRunCode({
              code,
              sources: options.sources,
              state: yield* makeWatchState,
              decide: options.decide,
            });
            const startedAt = yield* Clock.currentTimeMillis;
            const watch: Watch = {
              view: {
                event: armed.event,
                startedAt: new Date(startedAt).toISOString(),
                expiresAt: new Date(startedAt + expiresInSeconds * 1000).toISOString(),
                arguments: input,
                ...(target === undefined ? {} : { params: armed.params }),
              },
              ended: yield* Deferred.make<string>(),
              finished: yield* Deferred.make<void>(),
            };
            const claim = Effect.acquireRelease(
              Effect.suspend(() => {
                if (watches.has(id))
                  return Effect.fail(
                    new Invalid({
                      message: "id is already open in this or another session; choose another id",
                    }),
                  );
                watches.set(id, watch);
                return Effect.void;
              }),
              () =>
                Effect.sync(() => watches.delete(id)).pipe(
                  Effect.andThen(Deferred.succeed(watch.finished, undefined)),
                ),
            );
            const until = Effect.raceFirst(
              Effect.sleep(`${expiresInSeconds} seconds`).pipe(Effect.as("Expired")),
              Deferred.await(watch.ended),
            );
            const wake = (
              text: string,
              onFailure: (error: { readonly message: string }) => Effect.Effect<void>,
            ) =>
              harness
                .wake(send, { session, id, event: armed.event, text })
                .pipe(Effect.catchTag("Harness.WakeFailed", onFailure));
            // Succeeds at the watch's first health report, or with the reason it ended first.
            const ready = yield* Deferred.make<string | undefined, Invalid | SourceError>();
            const notify = (notification: Notification) => {
              switch (notification.type) {
                case "active":
                case "error":
                  return Deferred.succeed(ready, undefined).pipe(Effect.asVoid);
                case "heartbeat":
                  return Effect.void;
                case "event":
                  return wake(wakeText(notification.data), (error) =>
                    Deferred.succeed(watch.ended, `Wake failed: ${error.message}`).pipe(
                      Effect.asVoid,
                    ),
                  );
                case "terminated":
                  if (notification.reason === "Stopped") return Effect.void;
                  return wake(`Watch ${id} ended: ${notification.reason}`, (error) =>
                    Effect.logWarning(`Termination wake failed: ${error.message}`),
                  );
              }
            };
            yield* claim.pipe(
              Effect.andThen(armed.run(notify, until, runCode)),
              Effect.onExit((exit) => {
                const warn =
                  Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
                    ? Effect.logWarning("Watch failed").pipe(
                        Effect.annotateLogs({
                          id,
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
            const params = watch.view.params;
            return {
              id,
              ...(ended === undefined ? { expiresAt: watch.view.expiresAt } : { ended }),
              ...(params === undefined ? {} : { params }),
            };
          }),
        ),
      ),
  );
};
