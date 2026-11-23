import type { DateTime, Effect, JsonSchema, Scope, Stream } from "effect";
import { Data, Schema } from "effect";

export const SourceName = Schema.NonEmptyString.pipe(Schema.brand("SourceName"));
export type SourceName = typeof SourceName.Type;

export interface Event {
  readonly id?: string;
  readonly occurredAt?: DateTime.Utc;
  readonly payload: Schema.JsonObject;
}

export type Health = Data.TaggedEnum<{
  Live: {};
  Degraded: { readonly reason: string };
}>;
export const Health = Data.taggedEnum<Health>();

export class Rejected extends Schema.TaggedError<Rejected>()("Source.Rejected", {
  message: Schema.String,
}) {}

export class Unavailable extends Schema.TaggedError<Unavailable>()("Source.Unavailable", {
  message: Schema.String,
}) {}

export type SourceError = Rejected | Unavailable;

export interface Feed {
  readonly events: Stream.Stream<Event, SourceError>;
  /** Emits the current health first, then each change. */
  readonly health: Stream.Stream<Health>;
}

export interface EventType {
  readonly name: string;
  readonly description: string;
  readonly payload: JsonSchema.JsonSchema;
  /** How to filter this event, for an OpenCode automation or for a watch's code. */
  readonly recipe?: { readonly Events: string; readonly Tools: string } | undefined;
  /** Present when a watch names its resource with a link or shorthand, which resolves to the subscription params. */
  readonly target?:
    | {
        readonly description: string;
        readonly resolve: (target: string) => Effect.Effect<Schema.JsonObject, SourceError>;
      }
    | undefined;
  /** Receives the params a target resolved to, or {} for an event without a target. */
  subscribe(params: Schema.JsonObject): Effect.Effect<Feed, SourceError, Scope.Scope>;
}

export class ToolError extends Schema.TaggedError<ToolError>()("Tool.Failed", {
  message: Schema.String,
}) {}

export interface Tool {
  readonly description: string;
  /** Decoded before a provider's run, once, from whichever transport called it. */
  readonly input: Schema.Decoder<unknown>;
  readonly run: (input: unknown) => Effect.Effect<Schema.Json, ToolError>;
}

export interface Source {
  readonly name: SourceName;
  readonly tools: Readonly<Record<string, Tool>>;
  readonly events: ReadonlyArray<EventType>;
  readonly instructions?: string;
}
