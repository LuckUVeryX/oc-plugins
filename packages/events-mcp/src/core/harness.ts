import type { ServerCapabilities } from "@modelcontextprotocol/server";
import type { Effect, Schema } from "effect";
import type { Integration } from "../core/integration.ts";

export type Send = (notification: {
  readonly method: string;
  readonly params: Record<string, unknown>;
}) => Effect.Effect<void>;

export interface Wake {
  /** The decoded watch args that this harness added, empty when it adds none. */
  readonly session: Record<string, Schema.Json>;
  readonly watchId: string;
  readonly event: string;
  readonly text: string;
}

/** The harness has its own watch protocol, such as OpenCode's events extension. */
export interface Events {
  readonly _tag: "Events";
  readonly capabilities: ServerCapabilities;
  readonly instructions: (integrations: ReadonlyArray<Integration>) => string;
}

/** The harness arms watches through tools and wakes the session itself. */
export interface Tools {
  readonly _tag: "Tools";
  readonly capabilities: ServerCapabilities;
  readonly instructions: (integrations: ReadonlyArray<Integration>) => string;
  /** Extra watch args, such as a session or thread to wake. */
  readonly watchParams: Schema.Struct<Schema.Struct.Fields>;
  readonly wake: (send: Send, wake: Wake) => Effect.Effect<void>;
}

export type Harness = Events | Tools;

export const commonInstructions = [
  "When a filter cannot tell whether an event deserves a wake, ask the decision model from prepare with tools.events.decide.",
  'When a watch ends with "<step> failed", fix that step and arm a new watch.',
];

export const integrationInstructions = (integrations: ReadonlyArray<Integration>) =>
  integrations.flatMap((integration) =>
    integration.instructions === undefined ? [] : [integration.instructions],
  );
