// Claude Code and Codex have no MCP events, so they arm watches through tools and the
// server wakes the session itself. Delete this file once they support MCP events.
import { fromJsonSchema, type McpServer } from "@modelcontextprotocol/server";
import { Cause, Deferred, Effect, Exit, Schema, type Scope } from "effect";
import { isObject } from "effect/Predicate";
import { inputSchemaDocument, Invalid } from "./arguments.ts";
import { text, toolResult } from "./result.ts";
import type { Arm } from "./server.ts";
import type { SourceError } from "../source/contract.ts";
import type { Notification } from "../watch/watch.ts";
import type { Send, Tools } from "../harness/harness.ts";

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

export const registerTools = (options: {
  readonly server: McpServer;
  readonly harness: Tools;
  /** Owns every watch this connection arms; closing it ends them. */
  readonly scope: Scope.Scope;
  readonly run: <A, E>(effect: Effect.Effect<A, E>) => Promise<A>;
  readonly arm: Arm;
  readonly catalogue: () => ReadonlyArray<unknown>;
  readonly toolPaths: ReadonlyArray<string>;
  readonly defaultExpiresInSeconds: number;
}) => {
  const { server, harness, run, arm } = options;
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
            const armed = yield* arm({ name, args, view: input });
            const wake = (
              text: string,
              onFailure: (error: { readonly message: string }) => Effect.Effect<void>,
            ) =>
              harness
                .wake(send, { session, id: armed.id, event: armed.event, text })
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
                  return wake(notification.data, (error) =>
                    Deferred.succeed(armed.watch.ended, `Wake failed: ${error.message}`).pipe(
                      Effect.asVoid,
                    ),
                  );
                case "terminated":
                  if (notification.reason === "Stopped") return Effect.void;
                  return wake(`Watch ${armed.id} ended: ${notification.reason}`, (error) =>
                    Effect.logWarning(`Termination wake failed: ${error.message}`),
                  );
              }
            };
            yield* armed.run(notify).pipe(
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
        ),
      ),
  );
};
