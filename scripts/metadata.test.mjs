import { afterEach, expect, test } from "bun:test";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";

const fixtures = [];
afterEach(async () => {
  for (const fixture of fixtures.splice(0)) await rm(fixture, { recursive: true, force: true });
});

async function fixture() {
  const directory = await mkdtemp(join(tmpdir(), "oc-plugins-metadata-"));
  fixtures.push(directory);
  await mkdir(join(directory, "scripts"));
  await mkdir(join(directory, "packages", "example"), { recursive: true });
  await copyFile(
    new URL("./metadata.mjs", import.meta.url),
    join(directory, "scripts", "metadata.mjs"),
  );
  await copyFile(
    new URL("./test-install.mjs", import.meta.url),
    join(directory, "scripts", "test-install.mjs"),
  );
  const root = { name: "root", version: "1.2.3", devDependencies: { effect: "4.1.0" } };
  const pkg = {
    name: "example",
    version: "1.2.3",
    peerDependencies: { effect: "4.1.0" },
  };
  await writeFile(join(directory, "package.json"), JSON.stringify(root));
  const filename = join(directory, "packages", "example", "package.json");
  await writeFile(filename, JSON.stringify(pkg));
  return {
    directory,
    filename,
    pkg,
    run: (...args) =>
      spawnSync(process.execPath, ["scripts/metadata.mjs", ...args], {
        cwd: directory,
        encoding: "utf8",
      }),
  };
}

test("accepts matching manifests without writing them", async () => {
  const { filename, run } = await fixture();
  const original = await readFile(filename, "utf8");
  expect(run().status).toBe(0);
  expect(await readFile(filename, "utf8")).toBe(original);
});

test("rejects an existing package without a manifest", async () => {
  const { directory, filename, run } = await fixture();
  await rm(filename);
  const metadata = run();
  expect(metadata.status).toBe(1);
  expect(metadata.stderr).toContain("ENOENT");
  const install = spawnSync(process.execPath, ["scripts/test-install.mjs"], {
    cwd: directory,
    encoding: "utf8",
  });
  expect(install.status).toBe(1);
  expect(install.stderr).toContain("ENOENT");
});

test("reports drift without writing, then synchronizes idempotently", async () => {
  const { filename, pkg, run } = await fixture();
  pkg.version = "0.0.0";
  pkg.peerDependencies.effect = "^4.0.1";
  await writeFile(filename, JSON.stringify(pkg));
  expect(run().status).toBe(1);
  expect(JSON.parse(await readFile(filename, "utf8"))).toEqual(pkg);
  expect(run("--write").status).toBe(0);
  const written = await readFile(filename, "utf8");
  expect(JSON.parse(written)).toEqual({
    ...pkg,
    version: "1.2.3",
    peerDependencies: { effect: "4.1.0" },
  });
  expect(run("--write").status).toBe(0);
  expect(await readFile(filename, "utf8")).toBe(written);
  expect(run().status).toBe(0);
});

test("rejects an unmapped dependency without partially updating the manifest", async () => {
  const { filename, pkg, run } = await fixture();
  pkg.version = "0.0.0";
  pkg.dependencies = { missing: "1.0.0" };
  await writeFile(filename, JSON.stringify(pkg));
  const result = run("--write");
  expect(result.status).toBe(1);
  expect(result.stderr).toContain("missing has no root version");
  expect(JSON.parse(await readFile(filename, "utf8"))).toEqual(pkg);
});

test("rejects package names that differ from their directories", async () => {
  const { filename, pkg, run } = await fixture();
  pkg.name = "wrong";
  await writeFile(filename, JSON.stringify(pkg));
  const result = run();
  expect(result.status).toBe(1);
  expect(result.stderr).toContain("package name must match its directory");
});

test("rejects non-exact root versions instead of copying ranges into packages", async () => {
  const { directory, filename, run } = await fixture();
  const original = await readFile(filename, "utf8");
  await writeFile(
    join(directory, "package.json"),
    JSON.stringify({ name: "root", version: "1.2.3", devDependencies: { effect: "^4.1.0" } }),
  );
  const result = run("--write");
  expect(result.status).toBe(1);
  expect(result.stderr).toContain("effect needs an exact version");
  expect(await readFile(filename, "utf8")).toBe(original);
});

test("discovers additional packages and synchronizes every dependency group", async () => {
  const { directory, run } = await fixture();
  const added = join(directory, "packages", "another");
  await mkdir(added);
  await writeFile(
    join(added, "package.json"),
    JSON.stringify({
      name: "another",
      version: "0.0.0",
      dependencies: { effect: "catalog:" },
      peerDependencies: { effect: "workspace:*" },
      devDependencies: { effect: "4.0.1" },
      optionalDependencies: { effect: "^4.0.1" },
    }),
  );
  expect(run().status).toBe(1);
  expect(run("--write").status).toBe(0);
  expect(JSON.parse(await readFile(join(added, "package.json"), "utf8"))).toEqual({
    name: "another",
    version: "1.2.3",
    dependencies: { effect: "4.1.0" },
    peerDependencies: { effect: "4.1.0" },
    devDependencies: { effect: "4.1.0" },
    optionalDependencies: { effect: "4.1.0" },
  });
});
