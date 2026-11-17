import { strict as assert } from "node:assert";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript-sdk-schema";
import { createGenerator } from "ts-json-schema-generator";
import { JsonSchema, Schema, SchemaRepresentation } from "effect";

const here = fileURLToPath(new URL(".", import.meta.url));
const input = fileURLToPath(new URL("./sdk-event-handles.ts", import.meta.url));
const output = fileURLToPath(
  new URL("../../packages/lark-events-mcp/src/generated/events.json", import.meta.url),
);
const require = createRequire(import.meta.url);
const sdkPackage = require("@larksuiteoapi/node-sdk/package.json");
assert.equal(sdkPackage.version, "1.76.0", "Generator requires SDK 1.76.0");
assert.equal(ts.version, "5.9.3", "Generator requires TypeScript 5.9.3");

const program = ts.createProgram([input], {
  strict: true,
  skipLibCheck: true,
  types: [],
  module: ts.ModuleKind.NodeNext,
  moduleResolution: ts.ModuleResolutionKind.NodeNext,
  target: ts.ScriptTarget.ES2022,
});
const diagnostics = ts.getPreEmitDiagnostics(program);
if (diagnostics.length) {
  throw new Error(
    ts.formatDiagnosticsWithColorAndContext(diagnostics, {
      getCanonicalFileName: (file) => file,
      getCurrentDirectory: ts.sys.getCurrentDirectory,
      getNewLine: () => "\n",
    }),
  );
}
const declaration = program.getSourceFile(input)?.statements.find(ts.isTypeAliasDeclaration);
assert(declaration, "Missing SDK EventHandles alias");
const checker = program.getTypeChecker();
const eventType = checker.getTypeAtLocation(declaration);
const owners = eventType.getSymbol()?.getDeclarations();
assert(owners?.length, "Missing EventHandles interface declaration");
const members = eventType
  .getProperties()
  .filter((member) => member.getDeclarations()?.some((node) => owners.includes(node.parent)))
  .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
const fields = members.map((member) => {
  const handler = checker.getNonNullableType(
    checker.getTypeOfSymbolAtLocation(member, declaration),
  );
  const signatures = handler.getCallSignatures();
  const parameter = signatures[0]?.parameters[0];
  assert(
    signatures.length === 1 && parameter,
    `Expected one callback with a first argument: ${member.name}`,
  );
  const payload = checker.getTypeOfSymbolAtLocation(parameter, declaration);
  return `${JSON.stringify(member.name)}: ${checker.typeToString(
    payload,
    undefined,
    ts.TypeFormatFlags.NoTruncation | ts.TypeFormatFlags.InTypeAlias,
  )};`;
});

function rejectImpossible(value: unknown, path: string): void {
  if (value === false) throw new Error(`Impossible schema at ${path}`);
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    value.forEach((item, index) => rejectImpossible(item, `${path}[${index}]`));
    return;
  }
  for (const [key, child] of Object.entries(value)) {
    if (
      key === "not" &&
      child &&
      typeof child === "object" &&
      !Array.isArray(child) &&
      Object.keys(child).length === 0
    ) {
      throw new Error(`Impossible schema at ${path}`);
    }
    // Boolean false has a valid meaning for additionalProperties and additionalItems.
    if (child === false && (key === "additionalProperties" || key === "additionalItems")) continue;
    rejectImpossible(child, `${path}.${key}`);
  }
}

const directory = await mkdtemp(`${here}.sdk-generation-`);
try {
  const path = `${directory}/payloads.ts`;
  await writeFile(path, `export interface SdkEventPayloads {\n${fields.join("\n")}\n}\n`);
  const generator = createGenerator({
    path,
    tsconfig: `${here}tsconfig.sdk-generation.json`,
    type: "SdkEventPayloads",
    additionalProperties: true,
    topRef: false,
    skipTypeCheck: false,
  });
  const root = generator.createSchema("SdkEventPayloads");
  assert(root.properties, "Missing generated payload schemas");
  assert.deepEqual(
    Object.keys(root.properties).sort(),
    members.map((member) => member.name),
  );
  const events = members.map((member) => {
    const payload = root.properties?.[member.name];
    assert(payload && typeof payload === "object", `Missing payload schema for ${member.name}`);
    const draft07 = { ...payload, ...(root.definitions ? { definitions: root.definitions } : {}) };
    rejectImpossible(draft07, member.name);
    const document = JsonSchema.fromSchemaDraft07(draft07);
    Schema.make(
      SchemaRepresentation.fromJsonSchemaDocument(
        JsonSchema.fromSchemaDraft2020_12(document.schema),
      ).ast,
    );
    const description = ts.displayPartsToString(member.getDocumentationComment(checker));
    return {
      name: member.name,
      ...(description ? { description } : {}),
      payloadSchema: document.schema,
    };
  });
  const text = `${JSON.stringify(
    {
      sdk: "@larksuiteoapi/node-sdk",
      sdkVersion: sdkPackage.version,
      source: "EventHandles direct callback first argument",
      schemaDialect: "https://json-schema.org/draft/2020-12/schema",
      events,
    },
    null,
    2,
  )}\n`;
  if (process.argv.includes("--check")) {
    const existing = await readFile(output, "utf8");
    assert.equal(existing, text, "Generated event catalog is out of date");
  } else {
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, text);
  }
  process.stderr.write(`Verified ${events.length} SDK event schemas.\n`);
} finally {
  await rm(directory, { recursive: true, force: true });
}
