import type { ServerCapabilities } from "@modelcontextprotocol/server";
import { type Effect, Schema } from "effect";
import type { Source } from "../source/contract.ts";

/** Sends a notification on the MCP connection that serves this harness. */
export type Send = (notification: {
  readonly method: string;
  readonly params: Record<string, unknown>;
}) => Effect.Effect<void>;

/** A harness could not deliver a wake, so the watch cannot continue. */
export class WakeFailed extends Schema.TaggedError<WakeFailed>()("Harness.WakeFailed", {
  message: Schema.String,
}) {}

export interface Wake {
  /** The decoded watch args that this harness added, empty when it adds none. */
  readonly session: Record<string, Schema.Json>;
  readonly id: string;
  readonly event: string;
  readonly text: string;
}

/** The harness has its own watch protocol, such as OpenCode's events extension. */
export interface Events {
  readonly _tag: "Events";
  readonly capabilities: ServerCapabilities;
  readonly instructions: (sources: ReadonlyArray<Source>) => string;
}

/** The harness arms watches through tools and wakes the session itself. */
export interface Tools {
  readonly _tag: "Tools";
  readonly capabilities: ServerCapabilities;
  readonly instructions: (sources: ReadonlyArray<Source>) => string;
  /** Extra watch args, such as a session or thread to wake. */
  readonly watchParams: Schema.Struct<Schema.Struct.Fields>;
  /** Runs once after the watch input decodes, before the watch arms. */
  readonly check?: (session: Record<string, Schema.Json>) => Effect.Effect<void, WakeFailed>;
  readonly wake: (send: Send, wake: Wake) => Effect.Effect<void, WakeFailed>;
}

export type Harness = Events | Tools;

export const listEventsInstructions = [
  "Call list_events to see what this server can watch, with each event's arguments and payload. Arm one with the watch tool. The watch runs its code on each event, and whatever the code returns, unless null, reaches this session.",
  "A watch is live. It sees events from the moment it arms until it expires or you stop it, and nothing older is replayed. Choose an id. The `watches` tool shows the arguments of every open watch, and `unwatch` ends one early.",
  "When plain code cannot tell whether an event deserves a wake, ask the decision model from the code with tools.events.decide.",
  'When a watch ends with "code failed", fix the code and arm a new watch.',
].join("\n\n");

export const instructions =
  (options: { readonly arm: string; readonly wakes?: ReadonlyArray<string> }) =>
  (sources: ReadonlyArray<Source>): string =>
    [
      options.arm,
      ...sources.flatMap((source) =>
        source.instructions === undefined ? [] : [source.instructions],
      ),
      ...(options.wakes ?? []),
    ].join("\n\n");
