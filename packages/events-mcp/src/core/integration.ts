import type { DateTime, Effect, JsonSchema, Scope, Stream } from "effect";
import { Data, Schema } from "effect";

export const IntegrationName = Schema.NonEmptyString.pipe(Schema.brand("IntegrationName"));
export type IntegrationName = typeof IntegrationName.Type;

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

export class Rejected extends Schema.TaggedError<Rejected>()("Integration.Rejected", {
  message: Schema.String,
}) {}

export class Unavailable extends Schema.TaggedError<Unavailable>()("Integration.Unavailable", {
  message: Schema.String,
}) {}

export type IntegrationError = Rejected | Unavailable;

export interface Feed {
  readonly events: Stream.Stream<Event, IntegrationError>;
  /** Emits the current health first, then each change. */
  readonly health: Stream.Stream<Health>;
}

export interface EventKind {
  readonly name: string;
  readonly description: string;
  /** An object JSON Schema in the Effect-supported Draft 2020-12 subset. */
  readonly params: JsonSchema.JsonSchema;
  readonly payload: JsonSchema.JsonSchema;
  readonly target?:
    | {
        readonly description: string;
        readonly resolve: (target: string) => Effect.Effect<Schema.JsonObject, IntegrationError>;
      }
    | undefined;
  subscribe(params: Schema.JsonObject): Effect.Effect<Feed, IntegrationError, Scope.Scope>;
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

export interface Integration {
  readonly name: IntegrationName;
  readonly tools: Readonly<Record<string, Tool>>;
  readonly events: ReadonlyArray<EventKind>;
  readonly instructions?: string;
}
