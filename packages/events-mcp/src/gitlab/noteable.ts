import { Effect, Option, Schema } from "effect";
import { Rejected, type SourceError, Unavailable } from "../core/source.ts";
import type { GitlabClient } from "./client.ts";

export const NoteableType = Schema.Literals(["merge_request", "issue"]);
export type NoteableType = typeof NoteableType.Type;

/** The MergeRequest global ID prefix, shared by the noteable kinds and the source's param checks. */
export const mergeRequestGlobalId = "gid://gitlab/MergeRequest/";

interface NoteableKind {
  readonly label: string;
  readonly field: "mergeRequest" | "issue";
  readonly globalIdPrefix: string;
  readonly query: string;
  readonly globalIdHint: string;
  readonly databaseIdHint: string;
}

const kinds: Readonly<Record<NoteableType, NoteableKind>> = {
  merge_request: {
    label: "Merge request",
    field: "mergeRequest",
    globalIdPrefix: mergeRequestGlobalId,
    query: "query($id: MergeRequestID!) { mergeRequest(id: $id) { id iid projectId } }",
    globalIdHint: "Pass the global ID gid://gitlab/MergeRequest/<database id>, not the !iid.",
    databaseIdHint:
      "Pass the database id from GET /projects/:id/merge_requests/:iid -> .id, not the iid.",
  },
  issue: {
    label: "Issue",
    field: "issue",
    globalIdPrefix: "gid://gitlab/Issue/",
    query: "query($id: IssueID!) { issue(id: $id) { id iid projectId } }",
    globalIdHint: "Pass the global ID gid://gitlab/Issue/<database id>, not the #iid.",
    databaseIdHint: "Pass the database id from GET /projects/:id/issues/:iid -> .id, not the iid.",
  },
};

const Noteable = Schema.Struct({
  id: Schema.String,
  iid: Schema.String,
  projectId: Schema.Int,
});

const NoteableResponse = Schema.Struct({
  mergeRequest: Schema.optionalKey(Schema.NullOr(Noteable)),
  issue: Schema.optionalKey(Schema.NullOr(Noteable)),
});

interface ResolvedNoteable {
  readonly label: string;
  readonly iid: number;
  readonly projectId: number;
}

export const globalIdFor = (type: NoteableType, databaseId: number): string =>
  `${kinds[type].globalIdPrefix}${databaseId}`;

export const databaseIdHint = (type: NoteableType): string => kinds[type].databaseIdHint;

export const globalIdHint = (type: NoteableType): string => kinds[type].globalIdHint;

export const readNoteable = (
  client: GitlabClient,
  type: NoteableType,
  globalId: string,
  hint: string,
): Effect.Effect<ResolvedNoteable, SourceError> =>
  Effect.gen(function* () {
    const kind = kinds[type];
    const data = yield* client.graphql({ query: kind.query, variables: { id: globalId } }).pipe(
      Effect.catchTag("Source.Rejected", (error) =>
        Effect.fail(
          new Rejected({
            message: `Could not read ${kind.label.toLowerCase()} ${globalId}: ${error.message}`,
          }),
        ),
      ),
    );
    const decoded = Schema.decodeUnknownOption(NoteableResponse)(data);
    if (Option.isNone(decoded))
      return yield* Effect.fail(
        new Unavailable({ message: "GitLab returned an unexpected noteable lookup" }),
      );
    const noteable = decoded.value[kind.field];
    if (noteable === null || noteable === undefined)
      return yield* Effect.fail(
        new Rejected({
          message: `${kind.label} ${globalId} was not found or is not accessible with this GitLab token. ${hint}`,
        }),
      );
    const iid = Number(noteable.iid);
    if (!Number.isInteger(iid))
      return yield* Effect.fail(
        new Unavailable({ message: `GitLab returned an invalid iid for ${globalId}` }),
      );
    return { label: kind.label, iid, projectId: noteable.projectId };
  });
