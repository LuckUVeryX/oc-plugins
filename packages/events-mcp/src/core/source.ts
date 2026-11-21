import type { DateTime, Effect, JsonSchema, Scope, Stream } from "effect";
import { Data, Schema } from "effect";

export const SourceName = Schema.NonEmptyString.pipe(Schema.brand("SourceName"));
export type SourceName = typeof SourceName.Type;

export interface Occurrence {
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

export interface Subscription {
  readonly occurrences: Stream.Stream<Occurrence, SourceError>;
  /** Emits the current health first, then each change. */
  readonly health: Stream.Stream<Health>;
}

export interface EventType {
  readonly name: string;
  readonly description: string;
  /** An object JSON Schema in the Effect-supported Draft 2020-12 subset. */
  readonly params: JsonSchema.JsonSchema;
  readonly payload: JsonSchema.JsonSchema;
  subscribe(params: Schema.JsonObject): Effect.Effect<Subscription, SourceError, Scope.Scope>;
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
