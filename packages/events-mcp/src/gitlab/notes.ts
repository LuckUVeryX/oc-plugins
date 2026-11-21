import { Effect, Option, Schema } from "effect";
import { type EventType, type Occurrence, Rejected } from "../core/source.ts";
import { type Cable, type CableIdentifier } from "./cable.ts";
import type { GitlabClient } from "./client.ts";
import { databaseIdHint, globalIdFor, NoteableType, readNoteable } from "./noteable.ts";
import { subscribeToChannel } from "./subscription.ts";

const NotesParams = Schema.Struct({
  projectId: Schema.Int,
  noteableType: NoteableType,
  noteableId: Schema.Int,
});
type NotesParams = Schema.Schema.Type<typeof NotesParams>;

const NotesMessage = Schema.Struct({ event: Schema.String });

const NotesPayload = Schema.Struct({
  event: Schema.String,
  projectId: Schema.Int,
  noteableType: NoteableType,
  noteableId: Schema.Int,
  noteableIid: Schema.Int,
});

const payload = Schema.toJsonSchemaDocument(NotesPayload, { onExcessProperty: "error" }).schema;

export const notesRecipe = `const START = 0;
const { projectId, noteableType, noteableIid } = input.event.data;
const collection = noteableType === "merge_request" ? "merge_requests" : "issues";
const notes = await tools.gitlab.get({
  path: "/projects/" + projectId + "/" + collection + "/" + noteableIid + "/notes",
  query: { sort: "desc", order_by: "created_at", per_page: 20 },
});
const comments = notes.filter((note) => !note.system);
const previous = (await tools.watch.state.get()) ?? START;
const fresh = comments.filter((note) => note.id > previous);
await tools.watch.state.set(comments.reduce((max, note) => Math.max(max, note.id), previous));
return fresh.length === 0 ? null : { notes: fresh };`;

const description = `A note was added, changed, or removed on a merge request or issue.

Params: projectId (database id), noteableType ("merge_request" or "issue"), noteableId (database id).

Prepare recipe. Set \`START\` to the newest non-system note id from the request below, or \`0\` if it returns none. It reads the newest 20 notes and keeps the highest comment id in tools.watch.state. A signal with nothing newer than START or the stored id returns null. A burst of more than 20 comments between two signals skips the older ones.

${notesRecipe}`;

const identifier = (params: NotesParams): CableIdentifier => ({
  channel: "Noteable::NotesChannel",
  project_id: params.projectId,
  group_id: null,
  noteable_type: params.noteableType,
  noteable_id: params.noteableId,
});

const toOccurrence = (
  params: NotesParams,
  noteableIid: number,
  message: Schema.Json,
): Effect.Effect<Option.Option<Occurrence>> =>
  Effect.gen(function* () {
    const decoded = Schema.decodeUnknownOption(NotesMessage)(message);
    if (Option.isNone(decoded)) {
      yield* Effect.logWarning("Ignoring a malformed message on the GitLab notes channel");
      return Option.none();
    }
    return Option.some({
      payload: {
        event: decoded.value.event,
        projectId: params.projectId,
        noteableType: params.noteableType,
        noteableId: params.noteableId,
        noteableIid,
      },
    });
  });

const params = Schema.toJsonSchemaDocument(NotesParams, { onExcessProperty: "error" }).schema;

export interface NotesEventOptions {
  readonly client: GitlabClient;
  readonly cable: Cable;
}

export const makeNotesEvent = (options: NotesEventOptions): EventType => ({
  name: "noteable.notes_changed",
  description,
  params,
  payload,
  subscribe: (input) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(NotesParams)(input).pipe(
        Effect.mapError((issue) => new Rejected({ message: issue.message })),
      );
      const noteable = yield* readNoteable(
        options.client,
        decoded.noteableType,
        globalIdFor(decoded.noteableType, decoded.noteableId),
        databaseIdHint(decoded.noteableType),
      );
      if (noteable.projectId !== decoded.projectId)
        return yield* Effect.fail(
          new Rejected({
            message: `${noteable.label} ${decoded.noteableId} is in project ${noteable.projectId}, not ${decoded.projectId}. Pass the project database id from GET /projects/:id -> .id.`,
          }),
        );
      return yield* subscribeToChannel({
        cable: options.cable,
        identifier: identifier(decoded),
        toOccurrence: (message) => toOccurrence(decoded, noteable.iid, message),
      });
    }),
});
