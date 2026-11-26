import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../", import.meta.url));
const policy = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const env = { ...process.env };
delete env.NODE_PATH;
delete env.NODE_OPTIONS;
delete env.BUN_OPTIONS;
const temporary = await mkdtemp(
  join(existsSync("/tmp/opencode") ? "/tmp/opencode" : tmpdir(), "oc-plugins-install-"),
);
const unscoped = /^(?:@[^/]+\/)?([^/]+)$/;

function run(command, args, cwd, stage) {
  const result = spawnSync(command, args, {
    cwd,
    env,
    encoding: "utf8",
    timeout: 120_000,
  });
  if (result.error || result.status !== 0) {
    throw new Error(
      `${stage}: ${command} ${args.join(" ")} exited ${result.status}\n${result.stdout ?? ""}${result.stderr ?? ""}`,
      { cause: result.error },
    );
  }
  return result.stdout;
}

function exportTargets(exports) {
  if (exports === undefined) return [];
  if (typeof exports === "string") return [exports];
  assert(exports && typeof exports === "object", "Missing or unsupported exports");
  return Object.values(exports).flatMap(exportTargets);
}

function binTargets(bin) {
  if (typeof bin === "string") return [bin];
  assert(bin && typeof bin === "object", "Unsupported bin");
  return Object.values(bin);
}

try {
  assert(!temporary.startsWith(root), "Consumers must be outside the workspace");
  const packages = [];
  for (const entry of (await readdir(join(root, "packages"), { withFileTypes: true })).sort(
    (a, b) => a.name.localeCompare(b.name),
  )) {
    if (!entry.isDirectory()) continue;
    const directory = join(root, "packages", entry.name);
    const manifest = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
    assert.equal(unscoped.exec(manifest.name)?.[1], entry.name);
    assert.equal(manifest.version, policy.version);
    assert.deepEqual(
      manifest.devDependencies ?? {},
      {},
      `${manifest.name}: leaf development tools`,
    );
    for (const field of ["dependencies", "peerDependencies", "optionalDependencies"]) {
      for (const [name, version] of Object.entries(manifest[field] ?? {})) {
        assert.match(version, /^\d+\.\d+\.\d+(?:-[\w.-]+)?$/, `${manifest.name}: ${name}`);
        assert.equal(version, policy.devDependencies[name], `${manifest.name}: ${name} pin`);
      }
    }
    const artifact = join(temporary, `${entry.name}.tgz`);
    run(
      process.execPath,
      ["pm", "pack", "--filename", artifact],
      directory,
      `${manifest.name}: pack`,
    );
    const archive = await new Bun.Archive(await readFile(artifact)).files();
    const entries = [...archive.keys()];
    assert(
      entries.every((name) => !/(^|\/)(node_modules|test|tests)(\/|$)/.test(name)),
      `${manifest.name}: archive contains dependencies or tests`,
    );
    assert(archive.has("package/package.json"), `${manifest.name}: missing packed manifest`);
    const packed = JSON.parse(await archive.get("package/package.json").text());
    assert.deepEqual(packed, manifest, `${manifest.name}: pack rewrote the manifest`);
    for (const target of exportTargets(manifest.exports)) {
      assert(target.startsWith("./src/"), `${manifest.name}: export outside src`);
      assert(entries.includes(`package/${target.slice(2)}`), `${manifest.name}: missing ${target}`);
    }
    for (const target of manifest.bin ? binTargets(manifest.bin) : []) {
      assert(target.startsWith("./dist/"), `${manifest.name}: bin outside dist`);
      assert(entries.includes(`package/${target.slice(2)}`), `${manifest.name}: missing ${target}`);
    }
    packages.push({ manifest, artifact, directory: entry.name });
  }
  assert(packages.length > 0, "No packages discovered");

  for (const selection of [...packages.map((pkg) => [pkg]), packages]) {
    const label = selection.length === 1 ? selection[0].directory : "combined";
    const consumer = join(temporary, label);
    await mkdir(consumer);
    const dependencies = {};
    for (const { manifest, artifact } of selection) {
      Object.assign(dependencies, manifest.peerDependencies);
      dependencies[manifest.name] = artifact;
    }
    await writeFile(
      join(consumer, "package.json"),
      JSON.stringify({ private: true, type: "module", dependencies }),
    );
    await copyFile(
      new URL("./fixtures/install-consumer.mjs", import.meta.url),
      join(consumer, "verify.mjs"),
    );
    await copyFile(
      new URL("./fixtures/lark-sdk-preload.mjs", import.meta.url),
      join(consumer, "lark-sdk-preload.mjs"),
    );
    await writeFile(
      join(consumer, "expected.json"),
      JSON.stringify({
        packages: selection.map(({ manifest }) => manifest),
        versions: policy.devDependencies,
      }),
    );
    run(process.execPath, ["install", "--ignore-scripts"], consumer, `${label}: install`);
    const output = run(process.execPath, ["verify.mjs"], consumer, `${label}: behavior`);
    console.log(`PASS ${label}\n${output.trim()}`);
  }
  console.log("PASS independent and combined packed consumers");
} finally {
  await rm(temporary, { recursive: true, force: true });
}
