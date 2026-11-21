import { Effect, Option, Schema } from "effect";
import { type EventKind, type Event, Rejected } from "../core/integration.ts";
import { GitlabSocket, type ChannelIdentifier } from "./socket.ts";
import { GitlabClient } from "./client.ts";
import { databaseIdHint, globalIdFor, NoteableType, readNoteable } from "./noteable.ts";
import { subscribeToChannel } from "./channel.ts";

const NotesParams = Schema.Struct({
  projectId: Schema.Int.annotate({
    description: "Project database id, from the id field of GET /projects/:id.",
  }),
  noteableType: NoteableType.annotate({ description: `"merge_request" or "issue".` }),
  noteableId: Schema.Int.annotate({
    description:
      "Database id from the id field of GET /projects/:id/merge_requests/:iid, or the issue equivalent, not the iid.",
  }),
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

const description = `Watch for note changes on a merge request or issue. The recipe below wakes only for new non-system notes.

Pass projectId and noteableId as database ids, not iids or global ids, and noteableType as "merge_request" or "issue". GET /projects/:id/merge_requests/:iid returns the merge request's id and project_id, and the issue endpoint does the same.

Prepare recipe. Before you arm, read the newest notes to choose \`START\`. Use GET /projects/:projectId/merge_requests/:iid/notes with sort=desc, order_by=created_at, and per_page=20, or the issues endpoint. Set \`START\` to the newest non-system note's id, or 0 when that returns none. The recipe reads the newest 20 notes and keeps the highest comment id in tools.watch.state. A signal with nothing newer than START or the stored id returns null. More than 20 comments between two signals skips the older ones.

${notesRecipe}`;

const identifier = (params: NotesParams): ChannelIdentifier => ({
  channel: "Noteable::NotesChannel",
  project_id: params.projectId,
  group_id: null,
  noteable_type: params.noteableType,
  noteable_id: params.noteableId,
});

const toEvent = (
  params: NotesParams,
  noteableIid: number,
  message: Schema.Json,
): Effect.Effect<Option.Option<Event>> =>
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
  readonly client: GitlabClient.Interface;
  readonly socket: GitlabSocket.Interface;
}

export const makeNotesEvent = (options: NotesEventOptions): EventKind => ({
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
        socket: options.socket,
        identifier: identifier(decoded),
        toEvent: (message) => toEvent(decoded, noteable.iid, message),
      });
    }),
});
