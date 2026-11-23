import { Data, Effect, Schema } from "effect";
import { type EventType, type SourceError, Rejected, Unavailable } from "../source/contract.ts";
import type { GitlabClient } from "./client.ts";

export const NoteableType = Schema.Literals(["merge_request", "issue"]);

type Target = Data.TaggedEnum<{
  Project: { readonly project: string };
  Commit: { readonly project: string; readonly sha: string };
  MergeRequest: { readonly project: string; readonly iid: number };
  Issue: { readonly project: string; readonly iid: number };
  Pipeline: { readonly project: string; readonly id: number };
  Job: { readonly project: string; readonly id: number };
}>;
const Target = Data.taggedEnum<Target>();

type TargetTag = Target["_tag"];

const labels: Readonly<Record<TargetTag, string>> = {
  Project: "a project path",
  Commit: "a commit link",
  MergeRequest: "a merge request link",
  Issue: "an issue link",
  Pipeline: "a pipeline link",
  Job: "a job link",
};

const fullSha = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/i;

const positive = (value: string | undefined): number | undefined => {
  if (value === undefined || !/^\d+$/.test(value)) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined;
};

const decode = (value: string): string | undefined => {
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
};

const trimSlashes = (value: string): string => value.replace(/^\/+|\/+$/g, "");

interface ParseContext {
  readonly host: string;
  readonly name: string;
  readonly takes: string;
}

const invalid = (context: ParseContext, value: string): Rejected =>
  new Rejected({
    message: `Target "${value}" is not valid for ${context.name}. It takes ${context.takes}.`,
  });

const notLink = (context: ParseContext, value: string): Rejected =>
  new Rejected({
    message: `Target "${value}" must be an http or https link, a project path, or a shorthand. ${context.name} takes ${context.takes}.`,
  });

const projectPart = (
  value: string,
  context: ParseContext,
  input: string,
): Effect.Effect<string, SourceError> => {
  const project = trimSlashes(value);
  if (project === "" || /\s/.test(project)) return Effect.fail(invalid(context, input));
  return Effect.succeed(project);
};

const fromPath = (
  path: string,
  context: ParseContext,
  input: string,
): Effect.Effect<Target, SourceError> =>
  Effect.gen(function* () {
    const decoded = decode(path);
    if (decoded === undefined) return yield* Effect.fail(invalid(context, input));
    const marker = decoded.indexOf("/-/");
    if (marker === -1) {
      const project = yield* projectPart(decoded, context, input);
      return Target.Project({ project });
    }
    const project = yield* projectPart(decoded.slice(0, marker), context, input);
    const [kind, id] = trimSlashes(decoded.slice(marker + 3)).split("/");
    if (kind === undefined || id === undefined) return yield* Effect.fail(invalid(context, input));
    if (kind === "commit") {
      if (!fullSha.test(id))
        return yield* Effect.fail(
          new Rejected({
            message: `Target "${input}" needs the full commit SHA, 40 or 64 hex characters.`,
          }),
        );
      return Target.Commit({ project, sha: id.toLowerCase() });
    }
    const number = positive(id);
    if (number === undefined) return yield* Effect.fail(invalid(context, input));
    switch (kind) {
      case "merge_requests":
        return Target.MergeRequest({ project, iid: number });
      case "issues":
      case "work_items":
        return Target.Issue({ project, iid: number });
      case "pipelines":
        return Target.Pipeline({ project, id: number });
      case "jobs":
        return Target.Job({ project, id: number });
      default:
        return yield* Effect.fail(invalid(context, input));
    }
  });

const fromShorthand = (value: string, context: ParseContext): Effect.Effect<Target, SourceError> =>
  Effect.gen(function* () {
    const decoded = decode(value);
    if (decoded === undefined) return yield* Effect.fail(invalid(context, value));
    if (/^[^!#@]+[!#@]$/.test(decoded))
      return yield* Effect.fail(
        new Rejected({
          message: `Target "${value}" needs an iid or sha after the !, # or @.`,
        }),
      );
    const match = /^([^!#@]+)([!#@])([^!#@]+)$/.exec(decoded);
    if (match === null) {
      if (!decoded.includes("/") || /[\s\\]/.test(decoded))
        return yield* Effect.fail(invalid(context, value));
      return Target.Project({ project: trimSlashes(decoded) });
    }
    const [, rawProject = "", separator = "", rest = ""] = match;
    const project = yield* projectPart(rawProject, context, value);
    if (separator === "@") {
      if (!fullSha.test(rest))
        return yield* Effect.fail(
          new Rejected({
            message: `Target "${value}" needs the full commit SHA, 40 or 64 hex characters.`,
          }),
        );
      return Target.Commit({ project, sha: rest.toLowerCase() });
    }
    const number = positive(rest);
    if (number === undefined) return yield* Effect.fail(invalid(context, value));
    return separator === "!"
      ? Target.MergeRequest({ project, iid: number })
      : Target.Issue({ project, iid: number });
  });

const parseTarget = (options: {
  readonly host: string;
  readonly target: string;
  readonly context: ParseContext;
}): Effect.Effect<Target, SourceError> => {
  const value = options.target.trim();
  if (/^https?:\/\//i.test(value)) {
    let url: URL;
    try {
      url = new URL(value);
    } catch {
      return Effect.fail(invalid(options.context, options.target));
    }
    if (url.host.toLowerCase() !== options.host.toLowerCase())
      return Effect.fail(
        new Rejected({
          message: `Target ${url.href} is on ${url.host}; this server reads GitLab on ${options.host}.`,
        }),
      );
    return fromPath(url.pathname, options.context, options.target);
  }
  if (/^[a-z][a-z0-9+.-]*:/i.test(value) || value.startsWith("//"))
    return Effect.fail(notLink(options.context, options.target));
  if (value.includes("/-/")) return fromPath(value, options.context, options.target);
  return fromShorthand(value, options.context);
};

const IdResponse = Schema.Struct({ id: Schema.Int });
const CommitResponse = Schema.Struct({ id: Schema.String });
const NoteableResponse = Schema.Struct({ id: Schema.Int, project_id: Schema.Int });

const read = <A>(
  client: GitlabClient.Interface,
  schema: Schema.Decoder<A>,
  path: string,
  label: string,
): Effect.Effect<A, SourceError> =>
  client
    .get({ path })
    .pipe(
      Effect.flatMap((body) =>
        Schema.decodeUnknownEffect(schema)(body).pipe(
          Effect.mapError(
            () => new Unavailable({ message: `GitLab returned an unexpected ${label} response` }),
          ),
        ),
      ),
    );

const projectPath = (project: string, rest: string): string =>
  `/projects/${encodeURIComponent(project)}/${rest}`;

type Resolver<K extends TargetTag> = (
  target: Extract<Target, { readonly _tag: K }>,
  client: GitlabClient.Interface,
) => Effect.Effect<Schema.JsonObject, SourceError>;

type AnyResolver = (
  target: Target,
  client: GitlabClient.Interface,
) => Effect.Effect<Schema.JsonObject, SourceError>;

interface EventTarget {
  readonly takes: (host: string) => string;
  readonly description: (host: string) => string;
  readonly resolve: { readonly [K in TargetTag]?: Resolver<K> };
}

const mergeRequestTarget = (host: string): string =>
  `A merge request link such as https://${host}/group/project/-/merge_requests/123, or the shorthand group/project!123.`;

const mergeRequest: EventTarget = {
  takes: () => "a merge request link, or the shorthand group/project!123",
  description: mergeRequestTarget,
  resolve: {
    MergeRequest: (target, client) =>
      read(
        client,
        IdResponse,
        projectPath(target.project, `merge_requests/${target.iid}`),
        "merge request",
      ).pipe(Effect.map((value) => ({ issuableId: `gid://gitlab/MergeRequest/${value.id}` }))),
  },
};

const events: Readonly<Record<string, EventTarget>> = {
  ciPipelineStatusesUpdated: {
    takes: (host) =>
      `a project path such as group/project, a project link such as https://${host}/group/project, or a commit link such as https://${host}/group/project/-/commit/<full sha>`,
    description: (host) =>
      `A project path such as group/project, or a project link such as https://${host}/group/project, follows every pipeline in the project. A commit link such as https://${host}/group/project/-/commit/<full sha>, or the shorthand group/project@<full sha>, follows that commit's pipelines.`,
    resolve: {
      Project: (target, client) =>
        read(client, IdResponse, `/projects/${encodeURIComponent(target.project)}`, "project").pipe(
          Effect.as({ projectFullPath: target.project }),
        ),
      Commit: (target, client) =>
        read(
          client,
          CommitResponse,
          projectPath(target.project, `repository/commits/${target.sha}`),
          "commit",
        ).pipe(
          Effect.map((value) => ({
            projectFullPath: target.project,
            sha: value.id.toLowerCase(),
          })),
        ),
    },
  },
  ciPipelineStatusUpdated: {
    takes: (host) => `a pipeline link such as https://${host}/group/project/-/pipelines/123`,
    description: (host) => `A pipeline link such as https://${host}/group/project/-/pipelines/123.`,
    resolve: {
      Pipeline: (target, client) =>
        read(
          client,
          IdResponse,
          projectPath(target.project, `pipelines/${target.id}`),
          "pipeline",
        ).pipe(Effect.map((value) => ({ pipelineId: `gid://gitlab/Ci::Pipeline/${value.id}` }))),
    },
  },
  ciJobStatusUpdated: {
    takes: (host) => `a job link such as https://${host}/group/project/-/jobs/123`,
    description: (host) => `A job link such as https://${host}/group/project/-/jobs/123.`,
    resolve: {
      Job: (target, client) =>
        read(client, IdResponse, projectPath(target.project, `jobs/${target.id}`), "job").pipe(
          Effect.map((value) => ({ jobId: `gid://gitlab/Ci::Build/${value.id}` })),
        ),
    },
  },
  mergeRequestMergeStatusUpdated: mergeRequest,
  mergeRequestReviewersUpdated: mergeRequest,
  mergeRequestApprovalStateUpdated: mergeRequest,
  "noteable.notes_changed": {
    takes: () =>
      "a merge request link, an issue link, the shorthand group/project!123, or the shorthand group/project#123",
    description: () =>
      `A merge request or issue link, or a shorthand such as group/project!123 or group/project#123. An issue also works as a work item link under /-/work_items/.`,
    resolve: {
      MergeRequest: (target, client) =>
        read(
          client,
          NoteableResponse,
          projectPath(target.project, `merge_requests/${target.iid}`),
          "merge request",
        ).pipe(
          Effect.map((value) => ({
            projectId: value.project_id,
            noteableType: "merge_request",
            noteableId: value.id,
            noteableIid: target.iid,
          })),
        ),
      Issue: (target, client) =>
        read(
          client,
          NoteableResponse,
          projectPath(target.project, `issues/${target.iid}`),
          "issue",
        ).pipe(
          Effect.map((value) => ({
            projectId: value.project_id,
            noteableType: "issue",
            noteableId: value.id,
            noteableIid: target.iid,
          })),
        ),
    },
  },
};

export type TargetSupport = NonNullable<EventType["target"]>;

export const targetFor = (options: {
  readonly event: string;
  readonly host: string;
  readonly client: GitlabClient.Interface;
}): TargetSupport | undefined => {
  const entry = events[options.event];
  if (entry === undefined) return undefined;
  const name = `gitlab.${options.event}`;
  const context: ParseContext = {
    host: options.host,
    name,
    takes: entry.takes(options.host),
  };
  return {
    description: entry.description(options.host),
    resolve: (value) =>
      parseTarget({ host: options.host, target: value, context }).pipe(
        Effect.flatMap((target) =>
          Effect.gen(function* () {
            const resolve = entry.resolve[target._tag] as AnyResolver | undefined;
            if (resolve !== undefined) return yield* resolve(target, options.client);
            return yield* Effect.fail(
              new Rejected({
                message: `${name} takes ${context.takes}, not ${labels[target._tag]}.`,
              }),
            );
          }),
        ),
      ),
  };
};
