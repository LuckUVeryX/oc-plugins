import { Schema } from "effect";

/** The generated catalog's contract, decoded by both the generator and the integration. */
export const GitlabEvent = Schema.Struct({
  name: Schema.NonEmptyString,
  description: Schema.NonEmptyString,
  query: Schema.NonEmptyString,
  payload: Schema.JsonObject,
});
export type GitlabEvent = typeof GitlabEvent.Type;

const GitlabCatalog = Schema.Struct({ events: Schema.Array(GitlabEvent) });

export const decodeGitlabCatalog = Schema.decodeUnknownSync(GitlabCatalog);
