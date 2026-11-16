import { readFile, readdir, writeFile } from "node:fs/promises";
import { join, basename } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const manifest = JSON.parse(await readFile(join(root, "package.json"), "utf8"));
const exactVersion = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/;
const write = process.argv.includes("--write");
const groups = ["dependencies", "peerDependencies", "devDependencies", "optionalDependencies"];
const changes = [];
const updates = [];

if (process.argv.slice(2).some((arg) => arg !== "--write")) {
  throw new Error("Usage: bun scripts/metadata.mjs [--write]");
}
for (const [name, version] of Object.entries({
  [manifest.name]: manifest.version,
  ...manifest.devDependencies,
})) {
  if (!exactVersion.test(version))
    throw new Error(`${name} needs an exact version, got ${version}`);
}

for (const entry of await readdir(join(root, "packages"), { withFileTypes: true })) {
  if (!entry.isDirectory()) continue;
  const directory = join(root, "packages", entry.name);
  const filename = join(directory, "package.json");
  const pkg = JSON.parse(await readFile(filename, "utf8"));
  if (pkg.name !== basename(directory)) {
    throw new Error(`${filename}: package name must match its directory`);
  }
  let changed = false;
  if (pkg.version !== manifest.version) {
    changes.push(`${pkg.name}: version ${pkg.version} -> ${manifest.version}`);
    pkg.version = manifest.version;
    changed = true;
  }
  for (const group of groups) {
    for (const [name, version] of Object.entries(pkg[group] ?? {})) {
      const expected = manifest.devDependencies[name];
      if (expected === undefined) throw new Error(`${pkg.name}: ${name} has no root version`);
      if (version === expected) continue;
      changes.push(`${pkg.name}: ${group}.${name} ${version} -> ${expected}`);
      pkg[group][name] = expected;
      changed = true;
    }
  }
  if (changed) updates.push({ filename, pkg });
}

if (changes.length === 0) {
  console.log("Package versions and dependencies match the root manifest.");
} else if (write) {
  for (const { filename, pkg } of updates) {
    await writeFile(filename, `${JSON.stringify(pkg, null, 2)}\n`);
  }
  console.log(changes.join("\n"));
} else {
  console.error(
    `${changes.join("\n")}\nRun bun run sync:metadata to update the package manifests.`,
  );
  process.exitCode = 1;
}
