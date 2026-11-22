#!/usr/bin/env bun
// Target checks and the one write this skill performs: a GitLab pipeline.
// Reads VERIFY_GITLAB_* and VERIFY_LARK_* only. Never prints a token.
import { existsSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";

type Kind = "ok" | "warn" | "fail" | "skip";

const results: { kind: Kind; name: string; detail: string }[] = [];

const record = (kind: Kind, name: string, detail: string) => {
  results.push({ kind, name, detail });
  const label =
    kind === "ok" ? "ok  " : kind === "warn" ? "warn" : kind === "fail" ? "FAIL" : "skip";
  console.log(`${label}  ${name.padEnd(16)} ${detail}`);
};

const env = (name: string) => (process.env[name] ?? "").trim();

const run = (command: string[]) => {
  try {
    const result = Bun.spawnSync(command, { stdout: "pipe", stderr: "pipe" });
    return {
      code: result.exitCode ?? 1,
      out: new TextDecoder().decode(result.stdout).trim(),
      err: new TextDecoder().decode(result.stderr).trim(),
    };
  } catch {
    return { code: 127, out: "", err: "not found" };
  }
};

const host = env("VERIFY_GITLAB_HOST");
const project = env("VERIFY_GITLAB_PROJECT");
const ref = env("VERIFY_GITLAB_REF");
const larkId = env("VERIFY_LARK_APP_ID");
const larkSecret = env("VERIFY_LARK_APP_SECRET");
const larkChat = env("VERIFY_LARK_CHAT_ID");
const wantLark =
  process.argv.includes("--all") || larkId !== "" || larkSecret !== "" || larkChat !== "";
const wantCodex = process.argv.includes("--codex");

// bin/env is written by run.sh init with mode 600 and deleted by cleanup. The
// token is read from it, never printed.
const decodeRunValue = (raw: string) => {
  const match = /^'((?:[^']|'\\'')*)'$/.exec(raw);
  return match === null ? "" : match[1].replaceAll("'\\''", "'");
};

const tokenFromRun = () => {
  const dir = env("VERIFY_RUN_DIR");
  if (dir === "") return "";
  try {
    const text = readFileSync(join(dir, "bin", "env"), "utf8");
    const line = text.split("\n").find((item) => item.startsWith("GITLAB_TOKEN="));
    return line === undefined ? "" : decodeRunValue(line.slice("GITLAB_TOKEN=".length));
  } catch {
    return "";
  }
};

const token = () => {
  const given = env("GITLAB_TOKEN");
  if (given !== "") return given;
  const fromRun = tokenFromRun();
  if (fromRun !== "") return fromRun;
  if (host === "") return "";
  const glab = run(["glab", "config", "get", "token", "--host", host]);
  return glab.code === 0 ? glab.out : "";
};

let cache: string | undefined;
const gitlabToken = () => {
  cache ??= token();
  return cache;
};

const api = async (path: string, init?: RequestInit) => {
  const response = await fetch(`https://${host}/api/v4${path}`, {
    ...init,
    headers: {
      "PRIVATE-TOKEN": gitlabToken(),
      "content-type": "application/json",
      ...init?.headers,
    },
  });
  const text = await response.text();
  let body: unknown = null;
  try {
    body = JSON.parse(text);
  } catch {
    body = null;
  }
  return { status: response.status, body, text };
};

const object = (value: unknown) =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;

const str = (value: unknown) => (typeof value === "string" ? value : undefined);

const scalar = (value: unknown) =>
  typeof value === "string" || typeof value === "number" ? String(value) : undefined;

const checkLayout = () => {
  const hubDir = env("EVENTS_MCP_HUB_DIR");
  const runDir = env("VERIFY_RUN_DIR");
  if (hubDir === "") {
    record("fail", "hub dir", 'EVENTS_MCP_HUB_DIR is empty; run eval "$(scripts/run.sh init)"');
  } else if (!hubDir.startsWith("/tmp/verify-events-mcp/")) {
    record("fail", "hub dir", `${hubDir} is outside /tmp/verify-events-mcp`);
  } else if (runDir !== "" && hubDir !== join(runDir, "hub")) {
    record("fail", "hub dir", `${hubDir} is not the run hub ${join(runDir, "hub")}`);
  } else {
    record("ok", "hub dir", hubDir);
  }
  for (const [name, dir] of [
    ["run home", env("VERIFY_HOME")],
    ["codex home", env("VERIFY_CODEX_HOME")],
    ["work dir", env("VERIFY_WORK")],
  ] as const) {
    if (dir === "") record("warn", name, 'not set; run eval "$(scripts/run.sh init)"');
    else if (existsSync(dir)) record("ok", name, dir);
    else record("fail", name, `${dir} does not exist`);
  }
  const claudeAuth = join(env("VERIFY_HOME"), ".claude", ".credentials.json");
  if (env("VERIFY_HOME") === "") record("skip", "claude auth", "no run home");
  else if (existsSync(claudeAuth))
    record("ok", "claude auth", "credentials copied into the run home");
  else record("warn", "claude auth", "no copied credentials; the Claude feature cannot start");

  const codexHome = env("VERIFY_CODEX_HOME");
  const codexAuth = join(codexHome, "auth.json");
  if (codexHome === "") {
    record(wantCodex ? "fail" : "skip", "codex auth", "no codex home; run eval of run.sh init");
  } else if (existsSync(codexAuth)) {
    record("ok", "codex auth", "auth.json copied into the run home");
  } else if (wantCodex) {
    record(
      "fail",
      "codex auth",
      `no ${codexAuth}; fix: cp ~/.codex/auth.json '${codexAuth}' && chmod 600 '${codexAuth}'`,
    );
  } else {
    record(
      "warn",
      "codex auth",
      "no auth.json; the Codex feature cannot start (doctor --codex requires it)",
    );
  }

  const socket = env("VERIFY_TMUX_SOCKET");
  const expectedSocket = runDir === "" ? "" : `verify-${basename(runDir)}`;
  if (socket === "") {
    record("warn", "tmux socket", 'not set; run eval "$(scripts/run.sh init)"');
  } else if (expectedSocket !== "" && socket !== expectedSocket) {
    record("fail", "tmux socket", `${socket} is not this run socket ${expectedSocket}`);
  } else if (!socket.startsWith("verify-")) {
    record(
      "fail",
      "tmux socket",
      `${socket} is not a run socket; a shared socket lets one run kill another`,
    );
  } else {
    record("ok", "tmux socket", socket);
  }

  const bun = run(["bun", "--version"]);
  record(bun.out.startsWith("1.4.") ? "ok" : "warn", "bun", bun.out || bun.err);
  const tmux = run(["tmux", "-V"]);
  record(tmux.code === 0 ? "ok" : "fail", "tmux", tmux.out || tmux.err);
  const claude = run(["claude", "--version"]);
  record(claude.out.startsWith("2.1.") ? "ok" : "warn", "claude", claude.out || claude.err);
  const codex = run(["codex", "--version"]);
  record(codex.out.includes("0.171.") ? "ok" : "warn", "codex", codex.out || codex.err);
};

const checkGitlab = async () => {
  if (!/^[A-Za-z0-9.-]+(:[0-9]+)?$/.test(host)) {
    record("fail", "gitlab host", `VERIFY_GITLAB_HOST must be a bare host, got "${host}"`);
    return;
  }
  record("ok", "gitlab host", host);
  if (!/^[^/\s]+(\/[^/\s]+)+$/.test(project)) {
    record("fail", "gitlab project", `VERIFY_GITLAB_PROJECT must be a full path, got "${project}"`);
    return;
  }
  if (ref === "" || /\s/.test(ref)) {
    record("fail", "gitlab ref", "VERIFY_GITLAB_REF must be a branch or tag");
    return;
  }
  if (gitlabToken() === "") {
    record("fail", "gitlab token", "set GITLAB_TOKEN or authenticate glab for this host");
    return;
  }

  const user = await api("/user");
  const who = object(user.body);
  if (user.status !== 200 || who === undefined) {
    record("fail", "gitlab token", `GET /user returned ${user.status}; the token is not valid`);
    return;
  }
  record("ok", "gitlab token", `user ${scalar(who["id"])} ${str(who["username"])}`);

  const encoded = encodeURIComponent(project);
  const found = await api(`/projects/${encoded}`);
  const info = object(found.body);
  if (found.status !== 200 || info === undefined) {
    record("fail", "gitlab project", `GET /projects/${project} returned ${found.status}`);
    return;
  }
  const path = str(info["path_with_namespace"]) ?? "";
  const id = scalar(info["id"]) ?? "";
  if (path.toLowerCase() !== project.toLowerCase()) {
    record("fail", "gitlab project", `resolved ${path}, expected ${project}`);
    return;
  }
  record("ok", "gitlab project", `${path} (id ${id}, default ${str(info["default_branch"])})`);

  const branch = await api(`/projects/${id}/repository/branches/${encodeURIComponent(ref)}`);
  if (branch.status === 200) {
    const commit = object(object(branch.body)?.["commit"]);
    record("ok", "gitlab ref", `${ref} is a branch at ${str(commit?.["short_id"]) ?? "?"}`);
    return;
  }
  const tag = await api(`/projects/${id}/repository/tags/${encodeURIComponent(ref)}`);
  if (tag.status === 200) {
    record("ok", "gitlab ref", `${ref} is a tag`);
    return;
  }
  record("fail", "gitlab ref", `${ref} is not a branch or tag of ${project}`);
};

const checkLark = async () => {
  if (!wantLark) {
    record("skip", "lark", "no VERIFY_LARK_* set; pass --all to require it");
    return;
  }
  if (!/^cli_[0-9a-fA-F]{16}$/.test(larkId)) {
    record("fail", "lark app id", "VERIFY_LARK_APP_ID must be cli_ and 16 hex characters");
    return;
  }
  if (larkSecret === "") {
    record("fail", "lark secret", "VERIFY_LARK_APP_SECRET is empty");
    return;
  }
  if (!/^oc_[A-Za-z0-9]+$/.test(larkChat)) {
    record("fail", "lark chat", "VERIFY_LARK_CHAT_ID must be a chat id starting with oc_");
    return;
  }
  const auth = await fetch(
    "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal",
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ app_id: larkId, app_secret: larkSecret }),
    },
  );
  const authBody = object(await auth.json().catch(() => null));
  const tenant = str(authBody?.["tenant_access_token"]);
  if (auth.status !== 200 || tenant === undefined || tenant === "") {
    record(
      "fail",
      "lark app id",
      `token request returned ${auth.status} code ${str(authBody?.["code"])}`,
    );
    return;
  }
  record("ok", "lark app id", larkId);
  record("ok", "lark secret", `accepted, token expires in ${String(authBody?.["expire"] ?? "?")}s`);

  const chat = await fetch(
    `https://open.larksuite.com/open-apis/im/v1/chats/${encodeURIComponent(larkChat)}`,
    { headers: { authorization: `Bearer ${tenant}` } },
  );
  const chatBody = object(await chat.json().catch(() => null));
  const data = object(chatBody?.["data"]);
  const code = String(chatBody?.["code"] ?? "");
  if (chat.status !== 200 || code !== "0" || data === undefined) {
    record("fail", "lark chat", `GET chat returned ${chat.status} code ${code}`);
    return;
  }
  const mode = str(data["chat_mode"]) ?? "unknown";
  record(
    mode === "p2p" ? "ok" : "warn",
    "lark chat",
    `${larkChat} is a ${mode} chat${mode === "p2p" ? "" : "; the proof expects a p2p chat with the bot"}`,
  );
};

const doctor = async () => {
  checkLayout();
  await checkGitlab();
  await checkLark();
  const count = (kind: Kind) => results.filter((item) => item.kind === kind).length;
  console.log(
    `\ndoctor: ${count("ok")} ok, ${count("warn")} warnings, ${count("fail")} failures, ${count("skip")} skipped`,
  );
  if (count("fail") > 0) process.exit(1);
};

const trigger = async () => {
  if (host === "" || project === "" || ref === "") {
    console.error("trigger: set VERIFY_GITLAB_HOST, VERIFY_GITLAB_PROJECT, and VERIFY_GITLAB_REF");
    process.exit(2);
  }
  if (gitlabToken() === "") {
    console.error("trigger: set GITLAB_TOKEN or authenticate glab for this host");
    process.exit(2);
  }
  const encoded = encodeURIComponent(project);
  const found = await api(`/projects/${encoded}`);
  const info = object(found.body);
  const id = scalar(info?.["id"]);
  if (found.status !== 200 || id === undefined) {
    console.error(`trigger: cannot read ${project} (${found.status})`);
    process.exit(1);
  }
  const created = await api(`/projects/${id}/pipeline`, {
    method: "POST",
    body: JSON.stringify({ ref }),
  });
  const pipeline = object(created.body);
  const pipelineId = scalar(pipeline?.["id"]);
  if (created.status >= 300 || pipelineId === undefined) {
    console.error(
      `trigger: create pipeline returned ${created.status} ${created.text.slice(0, 300)}`,
    );
    process.exit(1);
  }
  console.log(
    JSON.stringify(
      {
        id: pipelineId,
        status: str(pipeline?.["status"]),
        ref: str(pipeline?.["ref"]),
        sha: str(pipeline?.["sha"]),
        web_url: str(pipeline?.["web_url"]),
      },
      null,
      2,
    ),
  );
};

const command = process.argv[2] ?? "doctor";
if (command === "doctor") await doctor();
else if (command === "trigger") await trigger();
else {
  console.error(`usage: target.ts doctor [--all] [--codex] | trigger`);
  process.exit(2);
}
