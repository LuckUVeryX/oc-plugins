import { Effect, Option, Result, Schedule, Schema, Stream } from "effect";
import {
  type Event,
  type EventType,
  Rejected,
  type Source,
  SourceName,
  Unavailable,
} from "../source/contract.ts";
import { CodemagicClient } from "./client.ts";
import { CodemagicSocket } from "./socket.ts";
import {
  makeArtifactsTool,
  makeCancelTool,
  makeGetTool,
  makeLogsTool,
  makeStartTool,
} from "./tools.ts";

const buildLink =
  /^https:\/\/codemagic\.io\/(?:app\/[0-9a-f]{24}\/)?build\/([0-9a-f]{24})\/?(?:[?#].*)?$/;
const appLink = /^https:\/\/codemagic\.io\/app\/([0-9a-f]{24})\/?(?:[?#].*)?$/;

const targetDescription =
  "A build link such as https://codemagic.io/app/<appId>/build/<buildId>, or an app link such as https://codemagic.io/app/<appId> for every build in the app.";

const Params = Schema.Union([
  Schema.Struct({ buildId: Schema.String }),
  Schema.Struct({ appId: Schema.String }),
]);

// The payload is Codemagic's build as GET /api/v3/builds/{id} returns it.
const payload = {
  type: "object",
  properties: {
    id: { type: "string" },
    app_id: { type: "string" },
    status: { type: "string" },
    branch: { type: ["string", "null"] },
    tag: { type: ["string", "null"] },
    commit: { type: ["object", "null"], description: "hash, message, url, author" },
    pull_request: {
      type: ["object", "null"],
      description: "number, source_branch, destination_branch",
    },
    workflow: { type: "object", description: "id, name, source" },
    artifacts: {
      type: "array",
      description:
        "name, size_in_bytes, short_lived_download_url. Anyone with the URL can download.",
    },
    labels: { type: "array", items: { type: "string" } },
    created_at: { type: "string" },
    started_at: { type: ["string", "null"] },
    finished_at: { type: ["string", "null"] },
  },
};

const retry = {
  schedule: Schedule.exponential("500 millis").pipe(Schedule.jittered, Schedule.upTo({ times: 5 })),
  while: (error: { readonly _tag: string }) => error._tag === "Source.Unavailable",
};

const buildStatusUpdated = (
  client: CodemagicClient.Interface,
  socket: CodemagicSocket.Interface,
): EventType => ({
  name: "buildStatusUpdated",
  description:
    "A build's status changes. In progress: initializing, queued, waiting, preparing, fetching, testing, building, publishing, finishing. Ended: finished, failed, canceled, timeout, skipped. The first watch on a build link also gets the current status.",
  payload,
  target: {
    description: targetDescription,
    resolve: Effect.fnUntraced(function* (target) {
      const buildId = buildLink.exec(target)?.[1];
      const appId = appLink.exec(target)?.[1];
      if (buildId === undefined && appId === undefined)
        return yield* Effect.fail(
          new Rejected({ message: `Target "${target}" is not valid. ${targetDescription}` }),
        );
      const found = yield* client.read(
        buildId === undefined ? `/apps/${appId}/workflows` : `/builds/${buildId}`,
      );
      if (Option.isNone(found))
        return yield* Effect.fail(
          new Rejected({
            message: `Target "${target}" does not exist, or CODEMAGIC_TOKEN cannot read it.`,
          }),
        );
      return buildId === undefined ? { appId: String(appId) } : { buildId };
    }),
  },
  subscribe: Effect.fnUntraced(function* (params) {
    const target = yield* Schema.decodeUnknownEffect(Params)(params).pipe(Effect.orDie);
    const { updates, health } = yield* socket.subscribe;
    const sent = new Map<string, string>();

    const readBuild = (buildId: string) =>
      client.read(`/builds/${buildId}`).pipe(
        Effect.flatMap((found) => Effect.fromOption(Option.map(found, (body) => body["data"]))),
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.JsonObject)),
        Effect.catchTags({
          NoSuchElementError: () => unreadable(buildId),
          SchemaError: () => unreadable(buildId),
        }),
        Effect.retry(retry),
      );

    // Codemagic repeats the status on step updates. A connect has no status, so the read supplies it.
    const toEvent = Effect.fnUntraced(function* (change: {
      readonly buildId: string;
      readonly status?: string | undefined;
    }) {
      if (change.status !== undefined && sent.get(change.buildId) === change.status)
        return Result.failVoid;
      const build = yield* readBuild(change.buildId);
      const status = change.status ?? String(build["status"]);
      if (sent.get(change.buildId) === status) return Result.failVoid;
      sent.set(change.buildId, status);
      return Result.succeed<Event>({ payload: { ...build, status } });
    });

    const changes = updates.pipe(
      Stream.filter((update) =>
        "buildId" in target
          ? update.buildId === target.buildId
          : update.context.appId === target.appId,
      ),
      Stream.map(({ buildId, payload }) => ({ buildId, status: payload.status })),
      Stream.filter(({ status }) => status !== undefined),
    );
    if ("buildId" in target) {
      const connects = health.pipe(
        Stream.filter((value) => value._tag === "Live"),
        Stream.map(() => ({ buildId: target.buildId, status: undefined })),
      );
      return {
        events: Stream.merge(changes, connects).pipe(Stream.filterMapEffect(toEvent)),
        health,
      };
    }
    return {
      events: changes.pipe(
        Stream.filterMapEffect((change) =>
          toEvent(change).pipe(
            Effect.catchTag("Source.Unavailable", (error) =>
              Effect.logWarning(`Skipping a Codemagic update: ${error.message}`).pipe(
                Effect.as(Result.failVoid),
              ),
            ),
          ),
        ),
      ),
      health,
    };
  }),
});

// Unavailable, because a build Codemagic just queued is missing for a short while.
const unreadable = (buildId: string) =>
  Effect.fail(new Unavailable({ message: `Codemagic does not return build ${buildId}` }));

export const source: Effect.Effect<
  Source,
  never,
  CodemagicClient.Service | CodemagicSocket.Service
> = Effect.gen(function* () {
  const client = yield* CodemagicClient.Service;
  const socket = yield* CodemagicSocket.Service;
  return {
    name: Schema.decodeUnknownSync(SourceName)("codemagic"),
    tools: {
      get: makeGetTool(client),
      start: makeStartTool(client),
      cancel: makeCancelTool(client),
      logs: makeLogsTool(client),
      artifacts: makeArtifactsTool(client),
    },
    events: [buildStatusUpdated(client, socket)],
    instructions:
      "Codemagic: codemagic.buildStatusUpdated sends every status change of a build, or of every build in an app. Filter on status, branch, or pull_request in code. Read more with codemagic_get and codemagic_logs, and start or cancel a build with codemagic_start and codemagic_cancel. codemagic_artifacts creates public download links.",
  };
});

export * as Codemagic from "./source.ts";
