import { Schema } from "effect";

/** The generated catalog's contract, decoded by both the generator and the source. */
export const GitlabEvent = Schema.Struct({
  name: Schema.NonEmptyString,
  description: Schema.NonEmptyString,
  params: Schema.JsonObject,
  query: Schema.NonEmptyString,
  payload: Schema.JsonObject,
  occurredAtField: Schema.optionalKey(Schema.NonEmptyString),
});
export type GitlabEvent = typeof GitlabEvent.Type;

export const GitlabCatalog = Schema.Struct({ events: Schema.Array(GitlabEvent) });

export const decodeGitlabCatalog = Schema.decodeUnknownSync(GitlabCatalog);
