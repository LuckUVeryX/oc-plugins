import { Effect, Option, Schema } from "effect";
import { type EventKind, type Event, Rejected } from "../core/integration.ts";
import { GitlabSocket, type ChannelIdentifier } from "./socket.ts";
import { NoteableType, type TargetSupport } from "./target.ts";
import { subscribeToChannel } from "./channel.ts";

/** What the target resolver reads from GitLab. The channel takes database ids. */
const NotesParams = Schema.Struct({
  projectId: Schema.Int,
  noteableType: NoteableType,
  noteableId: Schema.Int,
  noteableIid: Schema.Int,
});
type NotesParams = typeof NotesParams.Type;

const NotesMessage = Schema.Struct({ event: Schema.String });

const NotesPayload = Schema.Struct({ event: Schema.String, ...NotesParams.fields });

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

Arm it from a merge request or issue link, or a shorthand such as group/project!123 or group/project#123, by passing target. The payload carries projectId, noteableType, and noteableIid for the notes endpoint.

Script recipe. Before you arm, read the newest notes to choose \`START\`. Use GET /projects/:projectId/merge_requests/:iid/notes with sort=desc, order_by=created_at, and per_page=20, or the issues endpoint. Set \`START\` to the newest non-system note's id, or 0 when that returns none. The recipe reads the newest 20 notes and keeps the highest comment id in tools.watch.state. A signal with nothing newer than START or the stored id returns null. More than 20 comments between two signals skips the older ones.

${notesRecipe}`;

const identifier = (params: NotesParams): ChannelIdentifier => ({
  channel: "Noteable::NotesChannel",
  project_id: params.projectId,
  group_id: null,
  noteable_type: params.noteableType,
  noteable_id: params.noteableId,
});

const toEvent = (params: NotesParams, message: Schema.Json): Effect.Effect<Option.Option<Event>> =>
  Effect.gen(function* () {
    const decoded = Schema.decodeUnknownOption(NotesMessage)(message);
    if (Option.isNone(decoded)) {
      yield* Effect.logWarning("Ignoring a malformed message on the GitLab notes channel");
      return Option.none();
    }
    return Option.some({
      payload: { event: decoded.value.event, ...params },
    });
  });

interface NotesEventOptions {
  readonly socket: GitlabSocket.Interface;
  readonly target: TargetSupport | undefined;
}

export const makeNotesEvent = (options: NotesEventOptions): EventKind => ({
  name: "noteable.notes_changed",
  description,
  payload,
  target: options.target,
  subscribe: (input) =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(NotesParams)(input).pipe(
        Effect.mapError((issue) => new Rejected({ message: issue.message })),
      );
      return yield* subscribeToChannel({
        socket: options.socket,
        identifier: identifier(decoded),
        toEvent: (message) => toEvent(decoded, message),
      });
    }),
});
