import { strict as assert } from "node:assert";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { JsonSchema, Schema } from "effect";
import { decodeGitlabCatalog } from "../../packages/events-mcp/src/gitlab/catalog.ts";

const host = process.env["GITLAB_HOST"]?.trim() || "gitlab.example.com";
const output = fileURLToPath(
  new URL("../../packages/events-mcp/src/gitlab/events.json", import.meta.url),
);
const check = process.argv.includes("--check");

interface TypeRef {
  readonly kind: string;
  readonly name: string | null;
  readonly ofType?: TypeRef | null | undefined;
}

const TypeRef: Schema.Codec<TypeRef, TypeRef> = Schema.Struct({
  kind: Schema.String,
  name: Schema.NullOr(Schema.String),
  ofType: Schema.optionalKey(Schema.NullOr(Schema.suspend(() => TypeRef))),
});

const ArgumentDef = Schema.Struct({
  name: Schema.String,
  description: Schema.NullOr(Schema.String),
  type: TypeRef,
});
type ArgumentDef = typeof ArgumentDef.Type;

const FieldDef = Schema.Struct({
  name: Schema.String,
  description: Schema.NullOr(Schema.String),
  args: Schema.Array(ArgumentDef),
  type: TypeRef,
});
type FieldDef = typeof FieldDef.Type;

const TypeDef = Schema.Struct({
  kind: Schema.String,
  name: Schema.String,
  fields: Schema.optionalKey(Schema.NullOr(Schema.Array(FieldDef))),
  enumValues: Schema.optionalKey(
    Schema.NullOr(Schema.Array(Schema.Struct({ name: Schema.String }))),
  ),
});
type TypeDef = typeof TypeDef.Type;

const Envelope = Schema.Struct({
  data: Schema.optionalKey(
    Schema.Struct({
      __schema: Schema.Struct({ types: Schema.Array(Schema.Unknown) }),
    }),
  ),
  errors: Schema.optionalKey(Schema.Array(Schema.Struct({ message: Schema.String }))),
});

type Json = Schema.Json;
type JsonObject = Schema.JsonObject;

interface Selection {
  readonly scalars: ReadonlyArray<string>;
  readonly objects: Readonly<Record<string, ReadonlyArray<string>>>;
}

const PROJECT_FIELDS = ["id", "fullPath", "name", "webUrl"];
const USER_FIELDS = ["username", "name"];

const SELECTIONS = {
  Pipeline: {
    scalars: [
      "id",
      "iid",
      "name",
      "status",
      "ref",
      "sha",
      "source",
      "path",
      "duration",
      "failureReason",
      "createdAt",
      "updatedAt",
      "finishedAt",
    ],
    objects: {
      detailedStatus: ["name", "label", "text"],
      project: PROJECT_FIELDS,
      mergeRequest: ["id", "iid", "title", "state", "webUrl"],
      user: USER_FIELDS,
    },
  },
  CiJob: {
    scalars: [
      "id",
      "name",
      "status",
      "kind",
      "refName",
      "shortSha",
      "webPath",
      "duration",
      "queuedDuration",
      "failureMessage",
      "allowFailure",
      "retryable",
      "manualJob",
      "createdAt",
      "startedAt",
      "finishedAt",
    ],
    objects: {
      stage: ["name", "status"],
      project: PROJECT_FIELDS,
      pipeline: ["id", "iid", "path"],
    },
  },
  MergeRequest: {
    scalars: [
      "id",
      "iid",
      "name",
      "title",
      "state",
      "draft",
      "mergeStatusEnum",
      "detailedMergeStatus",
      "mergeable",
      "approved",
      "approvalsLeft",
      "approvalsRequired",
      "createdAt",
      "updatedAt",
      "mergedAt",
      "closedAt",
      "sourceBranch",
      "targetBranch",
      "diffHeadSha",
      "webUrl",
    ],
    objects: {
      project: PROJECT_FIELDS,
      author: USER_FIELDS,
      headPipeline: ["id", "iid", "status", "ref", "sha", "path", "updatedAt"],
    },
  },
} as const satisfies Record<string, Selection>;

interface EventConfig {
  readonly field: string;
  readonly selection: keyof typeof SELECTIONS;
  readonly fragment?: boolean;
  readonly requireOneOf?: ReadonlyArray<string>;
  readonly when: string;
}

const EVENTS: ReadonlyArray<EventConfig> = [
  {
    field: "ciPipelineStatusesUpdated",
    selection: "Pipeline",
    requireOneOf: ["projectId", "projectFullPath"],
    when: "Watch a project's pipeline status changes. Pass projectFullPath, such as group/project, or projectId. Pass sha to follow one commit.",
  },
  {
    field: "ciPipelineStatusUpdated",
    selection: "Pipeline",
    when: "Follow one pipeline through its status changes. Pass its global id as pipelineId.",
  },
  {
    field: "ciJobStatusUpdated",
    selection: "CiJob",
    when: "Follow one job through its status changes, for example while a retry runs. Pass its global id as jobId.",
  },
  {
    field: "userMergeRequestUpdated",
    selection: "MergeRequest",
    when: "Watch merge requests whose author, assignee, or reviewer is the token's user. Pass the token user's global id as userId.",
  },
  {
    field: "mergeRequestMergeStatusUpdated",
    selection: "MergeRequest",
    fragment: true,
    when: "Follow one merge request while GitLab recomputes whether it can merge. Pass its global id as issuableId.",
  },
  {
    field: "mergeRequestReviewersUpdated",
    selection: "MergeRequest",
    fragment: true,
    when: "Watch one merge request's reviewers, for example while you wait to be added as a reviewer. Pass its global id as issuableId.",
  },
  {
    field: "mergeRequestApprovalStateUpdated",
    selection: "MergeRequest",
    fragment: true,
    when: "Watch one merge request's approvals, for example while you wait for approval. Pass its global id as issuableId.",
  },
];

interface GlobalIdKind {
  readonly prefix: string;
  readonly source: string;
}

const GLOBAL_ID_KINDS: Readonly<Record<string, GlobalIdKind>> = {
  ProjectID: {
    prefix: "gid://gitlab/Project/",
    source: "the numeric id from GET /projects/:id",
  },
  CiPipelineID: {
    prefix: "gid://gitlab/Ci::Pipeline/",
    source: "the pipeline's numeric id from GET /projects/:id/pipelines/:pipeline_id",
  },
  CiBuildID: {
    prefix: "gid://gitlab/Ci::Build/",
    source: "the job's numeric id from GET /projects/:id/jobs/:job_id",
  },
  UserID: {
    prefix: "gid://gitlab/User/",
    source: "the token user's numeric id from GET /user",
  },
  IssuableID: {
    prefix: "gid://gitlab/MergeRequest/",
    source:
      "the merge request's database id from GET /projects/:id/merge_requests/:iid, not the iid",
  },
};

const PARAM_DESCRIPTIONS: Readonly<Record<string, string>> = {
  projectFullPath: "Project path as it appears in the web URL, such as group/project.",
};

const child = (ref: TypeRef): TypeRef => {
  if (ref.ofType === null || ref.ofType === undefined)
    throw new Error(`Missing ofType on ${ref.kind}`);
  return ref.ofType;
};

interface NamedRef {
  readonly kind: string;
  readonly name: string;
}

const named = (ref: TypeRef): NamedRef => {
  if (ref.kind === "NON_NULL" || ref.kind === "LIST") return named(child(ref));
  if (ref.name === null) throw new Error(`Missing name on ${ref.kind}`);
  return { kind: ref.kind, name: ref.name };
};

const isNonNull = (ref: TypeRef): boolean => ref.kind === "NON_NULL";

const globalIdKind = (argument: ArgumentDef) => GLOBAL_ID_KINDS[named(argument.type).name];

const globalIdParamDescription = ({ prefix, source }: GlobalIdKind): string =>
  `Use ${prefix}<id>. Replace <id> with ${source}.`;

const graphqlType = (ref: TypeRef): string => {
  if (ref.kind === "NON_NULL") return `${graphqlType(child(ref))}!`;
  if (ref.kind === "LIST") return `[${graphqlType(child(ref))}]`;
  if (ref.name === null) throw new Error(`Missing name on ${ref.kind}`);
  return ref.name;
};

const fieldOf = (type: TypeDef, name: string): FieldDef => {
  const field = type.fields?.find((candidate) => candidate.name === name);
  if (field === undefined) throw new Error(`${type.name}.${name} is missing from the schema`);
  return field;
};

const scalarField = (type: TypeDef, name: string): FieldDef => {
  const field = fieldOf(type, name);
  if (field.args.some((argument) => isNonNull(argument.type)))
    throw new Error(`${type.name}.${name} requires arguments`);
  const resolved = named(field.type);
  if (resolved.kind !== "SCALAR" && resolved.kind !== "ENUM")
    throw new Error(`${type.name}.${name} is ${resolved.kind}, not a scalar or enum`);
  return field;
};

const objectField = (type: TypeDef, name: string): TypeDefRef => {
  const field = fieldOf(type, name);
  const resolved = named(field.type);
  if (resolved.kind !== "OBJECT" && resolved.kind !== "INTERFACE")
    throw new Error(`${type.name}.${name} is ${resolved.kind}, not an object or interface`);
  return { field, name: resolved.name };
};

interface TypeDefRef {
  readonly field: FieldDef;
  readonly name: string;
}

const propertySchema = (ref: TypeRef, types: (name: string) => TypeDef): JsonObject => {
  const resolved = named(ref);
  if (resolved.kind === "ENUM") {
    const definition = types(resolved.name);
    const values = (definition.enumValues ?? []).map((value) => value.name);
    if (values.length === 0) throw new Error(`Enum ${definition.name} has no values`);
    return { type: "string", enum: values };
  }
  if (resolved.kind !== "SCALAR") throw new Error(`Unhandled ${resolved.kind} ${resolved.name}`);
  switch (resolved.name) {
    case "Int":
      return { type: "integer" };
    case "Float":
      return { type: "number" };
    case "Boolean":
      return { type: "boolean" };
    case "ID":
    case "String":
    case "Time":
    case "Duration":
    case "Date":
      return { type: "string" };
    default:
      if (resolved.name.endsWith("ID")) return { type: "string" };
      throw new Error(`Unhandled scalar ${resolved.name}`);
  }
};

const selectionText = (selection: Selection): string =>
  [
    ...selection.scalars,
    ...Object.entries(selection.objects).map(
      ([name, scalars]) => `${name} { ${scalars.join(" ")} }`,
    ),
  ].join(" ");

const objectSchema = (
  properties: Record<string, Json>,
  required: ReadonlyArray<string>,
): JsonObject => ({
  type: "object",
  properties,
  ...(required.length === 0 ? {} : { required }),
});

const payloadSchema = (
  selectionType: TypeDef,
  selection: Selection,
  types: (name: string) => TypeDef,
): JsonObject => {
  const properties: Record<string, Json> = {};
  const required: Array<string> = [];
  for (const name of selection.scalars) {
    const field = scalarField(selectionType, name);
    properties[name] = propertySchema(field.type, types);
    if (isNonNull(field.type)) required.push(name);
  }
  for (const [name, children] of Object.entries(selection.objects)) {
    const { field, name: nestedName } = objectField(selectionType, name);
    const nestedType = types(nestedName);
    const nestedProperties: Record<string, Json> = {};
    const nestedRequired: Array<string> = [];
    for (const childName of children) {
      const childField = scalarField(nestedType, childName);
      nestedProperties[childName] = propertySchema(childField.type, types);
      if (isNonNull(childField.type)) nestedRequired.push(childName);
    }
    properties[name] = objectSchema(nestedProperties, nestedRequired);
    if (isNonNull(field.type)) required.push(name);
  }
  return objectSchema(properties, required);
};

const paramsSchema = (
  field: FieldDef,
  requireOneOf: ReadonlyArray<string> | undefined,
): JsonObject => {
  const properties: Record<string, Json> = {};
  const required: Array<string> = [];
  for (const argument of field.args) {
    const kind = globalIdKind(argument);
    const description = PARAM_DESCRIPTIONS[argument.name] ?? argument.description ?? "";
    properties[argument.name] =
      kind === undefined
        ? { type: "string", ...(description === "" ? {} : { description }) }
        : {
            type: "string",
            pattern: `^${kind.prefix}\\d+$`,
            description: globalIdParamDescription(kind),
          };
    if (isNonNull(argument.type)) required.push(argument.name);
  }
  return {
    ...objectSchema(properties, required),
    ...(requireOneOf === undefined
      ? {}
      : { anyOf: requireOneOf.map((name) => ({ required: [name] })) }),
    additionalProperties: false,
  };
};

const queryFor = (field: FieldDef, selection: string): string => {
  const declarations = field.args.map(
    (argument) => `$${argument.name}: ${graphqlType(argument.type)}`,
  );
  const passed = field.args.map((argument) => `${argument.name}: $${argument.name}`);
  const parameters = passed.length === 0 ? "" : `(${passed.join(", ")})`;
  const variables = declarations.length === 0 ? "" : `(${declarations.join(", ")})`;
  return `subscription${variables} { ${field.name}${parameters} { ${selection} } }`;
};

const generate = (raw: unknown) => {
  const envelope = Schema.decodeUnknownSync(Envelope)(raw);
  if (envelope.errors !== undefined && envelope.errors.length > 0)
    throw new Error(
      `GitLab introspection failed: ${envelope.errors.map((e) => e.message).join("; ")}`,
    );
  if (envelope.data === undefined) throw new Error("GitLab introspection returned no data");
  const rawTypes = envelope.data.__schema.types;
  const rawByName = new Map<string, unknown>();
  for (const type of rawTypes) {
    if (type === null || typeof type !== "object" || !("name" in type)) continue;
    if (typeof type.name === "string") rawByName.set(type.name, type);
  }
  const cache = new Map<string, TypeDef>();
  const types = (name: string): TypeDef => {
    const cached = cache.get(name);
    if (cached !== undefined) return cached;
    const rawType = rawByName.get(name);
    if (rawType === undefined) throw new Error(`${name} is missing from the schema`);
    const decoded = Schema.decodeUnknownSync(TypeDef)(rawType);
    cache.set(name, decoded);
    return decoded;
  };
  const subscription = types("Subscription");
  const fieldByName = (name: string): FieldDef => {
    const field = subscription.fields?.find((candidate) => candidate.name === name);
    if (field === undefined)
      throw new Error(
        `GitLab field ${name} is missing from the Subscription type; update the allowlist and the source`,
      );
    return field;
  };
  const allowed = EVENTS.map((event) => event.field);
  assert.equal(
    new Set(allowed).size,
    allowed.length,
    "The GitLab generator allowlist has duplicate fields",
  );
  const events = EVENTS.map((event) => {
    const field = fieldByName(event.field);
    const selection: Selection = SELECTIONS[event.selection];
    const returnType = named(field.type);
    if (event.fragment === true) {
      if (returnType.kind !== "UNION")
        throw new Error(`${event.field} returns ${returnType.kind}, not a union`);
      if (event.selection !== "MergeRequest")
        throw new Error(`${event.field} must select on MergeRequest`);
    } else if (returnType.name !== event.selection) {
      throw new Error(`${event.field} returns ${returnType.name}, not ${event.selection}`);
    }
    const selectionType = types(event.selection);
    const inner = selectionText(selection);
    const selectionSet = event.fragment === true ? `... on ${event.selection} { ${inner} }` : inner;
    const params = paramsSchema(field, event.requireOneOf);
    const payload = payloadSchema(selectionType, selection, types);
    return {
      name: event.field,
      description: event.when,
      params,
      query: queryFor(field, selectionSet),
      payload,
    };
  });
  return { events };
};

const verify = (text: string) => {
  const catalog = decodeGitlabCatalog(JSON.parse(text));
  assert.deepEqual(
    catalog.events.map((event) => event.name).sort(),
    EVENTS.map((event) => event.field).sort(),
    "Generated GitLab events no longer match the allowlist",
  );
  for (const event of catalog.events) {
    JsonSchema.fromSchemaDraft2020_12(event.params);
    assert.equal(event.params["type"], "object", `${event.name} params must be an object`);
    assert.equal(event.payload["type"], "object", `${event.name} payload must be an object`);
    assert.ok(event.query.startsWith("subscription"), `${event.name} query must be a subscription`);
  }
  return catalog;
};

if (check) {
  const existing = await readFile(output, "utf8");
  const catalog = verify(existing);
  process.stderr.write(`Verified ${catalog.events.length} GitLab event schemas.\n`);
} else {
  const token = process.env["GITLAB_TOKEN"]?.trim();
  if (!token)
    throw new Error(
      "GITLAB_TOKEN is required to regenerate the GitLab catalog; set it to a read_api token for " +
        host,
    );
  const TYPE_REF_SELECTION =
    "type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }";
  const response = await fetch(`https://${host}/api/graphql`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      query: `query Introspection {
  __schema {
    types {
      kind
      name
      fields {
        name
        description
        args {
          name
          description
          ${TYPE_REF_SELECTION}
        }
        ${TYPE_REF_SELECTION}
      }
      enumValues { name }
    }
  }
}`,
    }),
  });
  if (!response.ok)
    throw new Error(`GitLab introspection failed: HTTP ${response.status} ${response.statusText}`);
  const text = `${JSON.stringify(generate(await response.json()), null, 2)}\n`;
  verify(text);
  await mkdir(dirname(output), { recursive: true });
  await writeFile(output, text);
  process.stderr.write(`Generated ${EVENTS.length} GitLab event schemas.\n`);
}
