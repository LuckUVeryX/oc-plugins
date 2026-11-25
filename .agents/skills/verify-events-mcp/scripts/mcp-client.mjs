#!/usr/bin/env bun
// Drives the events-mcp OpenCode surface over stdio, the same MCP connection an
// OpenCode session opens. Modes: list, read-tools, watch, hub.
import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport } from "@modelcontextprotocol/client/stdio";

const HERE = dirname(fileURLToPath(import.meta.url));
const REPO = join(HERE, "../../../..");
const ENTRY = join(REPO, "packages/events-mcp/src/main.ts");
const EVIDENCE = process.env.VERIFY_EVIDENCE ?? "";
if (EVIDENCE !== "") mkdirSync(EVIDENCE, { recursive: true });

// The run's credentials live in its bin/env, written by run.sh init with mode
// 600. Read the token from there when the shell has none; never print it.
const runValue = (name) => {
  const dir = process.env.VERIFY_RUN_DIR ?? "";
  if (dir === "") return "";
  try {
    const text = readFileSync(join(dir, "bin", "env"), "utf8");
    const line = text.split("\n").find((item) => item.startsWith(`${name}=`));
    if (line === undefined) return "";
    const match = /^'((?:[^']|'\\'')*)'$/.exec(line.slice(name.length + 1));
    return match === null ? "" : match[1].replaceAll("'\\''", "'");
  } catch {
    return "";
  }
};

const SUB = "io.modelcontextprotocol/subscriptionId";
const loose = {
  "~standard": { version: 1, vendor: "verify-events-mcp", validate: (value) => ({ value }) },
};

const env = () => {
  const value = process.env.VERIFY_GITLAB_HOST ?? "";
  if (value === "") throw new Error("VERIFY_GITLAB_HOST is empty");
  const out = {
    ...process.env,
    EVENTS_MCP_HUB_DIR: process.env.EVENTS_MCP_HUB_DIR ?? "",
    GITLAB_HOST: value,
  };
  const token = process.env.GITLAB_TOKEN || runValue("GITLAB_TOKEN");
  if (token !== "") out.GITLAB_TOKEN = token;
  if (process.env.VERIFY_LARK_APP_ID) out.LARK_APP_ID = process.env.VERIFY_LARK_APP_ID;
  if (process.env.VERIFY_LARK_APP_SECRET) out.LARK_APP_SECRET = process.env.VERIFY_LARK_APP_SECRET;
  return out;
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const write = (name, value) => {
  if (EVIDENCE === "") {
    console.log(`(no VERIFY_EVIDENCE; ${name} not saved)`);
    return;
  }
  mkdirSync(EVIDENCE, { recursive: true });
  writeFileSync(join(EVIDENCE, name), JSON.stringify(value, null, 2) + "\n");
};

const connect = async (tag) => {
  const transport = new StdioClientTransport({
    command: "bun",
    args: [ENTRY, "opencode"],
    cwd: REPO,
    env: env(),
    stderr: "pipe",
  });
  if (EVIDENCE !== "")
    transport.stderr?.on("data", (chunk) =>
      appendFileSync(join(EVIDENCE, `${tag}-stderr.log`), String(chunk)),
    );
  const client = new Client(
    { name: `verify-${tag}`, version: "0" },
    { versionNegotiation: { mode: "auto" } },
  );
  await client.connect(transport);
  return { client, transport };
};

const call = (client, method, params, timeout = 60_000) =>
  client.request({ method, params }, loose, { timeout });

const tool = async (client, name, args) => {
  const response = await call(client, "tools/call", { name, arguments: args });
  const text = response.content?.[0]?.text ?? "null";
  const body = JSON.parse(text);
  if (response.isError) throw new Error(`${name}: ${body.message ?? text}`);
  return body;
};

const projectPath = () => {
  const value = process.env.VERIFY_GITLAB_PROJECT ?? "";
  if (!value) throw new Error("VERIFY_GITLAB_PROJECT is empty");
  return value;
};

const list = async () => {
  const { client } = await connect("list");
  const events = await call(client, "events/list", {});
  const tools = await call(client, "tools/list", {});
  const protocol = client.getNegotiatedProtocolVersion();
  await client.close();
  const record = {
    protocol,
    events: events.events.map((event) => event.name),
    tools: tools.tools.map((item) => item.name),
  };
  write("opencode-list.json", record);
  console.log(
    JSON.stringify(
      {
        protocol: record.protocol,
        events: record.events.length,
        lark: record.events.filter((name) => name.startsWith("lark.")).length,
        gitlab: record.events.filter((name) => name.startsWith("gitlab.")).length,
        codemagic: record.events.filter((name) => name.startsWith("codemagic.")).length,
        tools: record.tools,
      },
      null,
      2,
    ),
  );
  if (record.events.length !== 23)
    throw new Error(`expected 23 events, got ${record.events.length}`);
};

const readTools = async () => {
  const { client } = await connect("read-tools");
  const user = await tool(client, "gitlab_get", { path: "/user" });
  const project = projectPath();
  const info = await tool(client, "gitlab_get", {
    path: `/projects/${encodeURIComponent(project)}`,
  });
  const record = {
    user: { id: user.id, username: user.username, state: user.state },
    project: {
      id: info.id,
      path_with_namespace: info.path_with_namespace,
      default_branch: info.default_branch,
    },
    lark: null,
  };
  if (process.env.VERIFY_LARK_CHAT_ID) {
    const chat = await tool(client, "lark_get", {
      path: `/im/v1/chats/${encodeURIComponent(process.env.VERIFY_LARK_CHAT_ID)}`,
    });
    record.lark = { chat_mode: chat.chat_mode };
  }
  await client.close();
  write("read-tools.json", record);
  console.log(JSON.stringify(record, null, 2));
  if (!record.user.username) throw new Error("gitlab_get /user returned no username");
  if (record.project.path_with_namespace.toLowerCase() !== project.toLowerCase())
    throw new Error(`project read returned ${record.project.path_with_namespace}`);
  if (record.lark !== null && record.lark.chat_mode !== "p2p")
    throw new Error(`lark_get read a ${record.lark.chat_mode} chat; expected p2p`);
};

const trigger = () => {
  const result = Bun.spawnSync(["bun", join(HERE, "target.ts"), "trigger"], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = new TextDecoder().decode(result.stdout);
  const err = new TextDecoder().decode(result.stderr);
  if (result.exitCode !== 0) throw new Error(`trigger failed: ${err.trim() || out.trim()}`);
  return JSON.parse(out);
};

const watch = async (options) => {
  const { client } = await connect("watch");
  const project = projectPath();
  const timeout = Number(options.timeout ?? 900) * 1000;
  const notifications = [];
  for (const type of ["active", "heartbeat", "event", "error", "terminated"])
    client.setNotificationHandler(`notifications/events/${type}`, { params: loose }, (params) =>
      notifications.push({
        type,
        at: new Date().toISOString(),
        params,
        subscriptionId: params?._meta?.[SUB],
      }),
    );

  const controller = new AbortController();
  const armedAt = new Date().toISOString();
  const done = client
    .request(
      {
        method: "events/stream",
        params: {
          name: "gitlab.ciPipelineStatusesUpdated",
          arguments: {
            target: project,
          },
        },
      },
      loose,
      { signal: controller.signal, timeout },
    )
    .then(
      () => "resolved",
      (error) => `rejected: ${error?.message ?? error}`,
    );

  const waitFor = async (predicate, limit, label) => {
    const start = Date.now();
    while (Date.now() - start < limit) {
      const found = notifications.find(predicate);
      if (found) return found;
      await sleep(100);
    }
    throw new Error(`no ${label} within ${Math.round(limit / 1000)}s`);
  };

  await waitFor((item) => item.type === "active", 60_000, "active");
  const subscriptionId = notifications.find((item) => item.type === "active")?.subscriptionId;
  const armed = { armedAt, subscriptionId, project };
  write("opencode-armed.json", armed);
  console.log(JSON.stringify({ armed }, null, 2));

  const started = options.trigger ? trigger() : null;
  if (started) {
    write("opencode-trigger.json", started);
    console.log(JSON.stringify({ triggered: started }, null, 2));
  }

  const event = await waitFor(
    (item) =>
      item.type === "event" &&
      (started === null || String(item.params?.data?.id).endsWith(`/${started.id}`)) &&
      (subscriptionId === undefined || item.subscriptionId === subscriptionId),
    timeout,
    "event",
  );
  const receivedAt = new Date().toISOString();
  const record = {
    project,
    subscriptionId,
    armedAt,
    triggered: started,
    receivedAt,
    event: {
      eventId: event.params?.eventId,
      name: event.params?.name,
      timestamp: event.params?.timestamp,
      data: event.params?.data,
    },
    notifications: notifications.map((item) => ({ type: item.type, at: item.at })),
  };
  write("opencode-wake.json", record);
  console.log(JSON.stringify(record, null, 2));

  controller.abort();
  await done;
  await sleep(500);
  await client.close();
  if (started && !String(record.event.data?.id).endsWith(`/${started.id}`))
    throw new Error(`event does not name the triggered pipeline ${started.id}`);
};

const hub = async (options) => {
  const count = Number(options.sessions ?? 2);
  const logPath = join(process.env.EVENTS_MCP_HUB_DIR ?? "", "state", "hub.log");
  const before = (
    await Bun.file(logPath)
      .text()
      .catch(() => "")
  ).length;
  const clients = await Promise.all(
    Array.from({ length: count }, (_, index) => connect(`hub-${index + 1}`)),
  );
  try {
    const sessions = [];
    for (const [index, entry] of clients.entries()) {
      const events = await call(entry.client, "events/list", {});
      sessions.push({
        session: index + 1,
        events: events.events.length,
      });
    }
    await sleep(1000);
    const log = await Bun.file(logPath).text();
    const tail = log.slice(before);
    const pidOf = (text) =>
      [...text.matchAll(/listening at .* as pid (\d+)/g)].map((match) => match[1]);
    const listening = pidOf(tail);
    const reused = listening.length === 0;
    const pids = [...new Set(reused ? pidOf(log).slice(-1) : listening)];
    const openedIds = [...tail.matchAll(/\(#(\d+)\): connection opened/g)].map((match) => match[1]);
    const closedIds = new Set(
      [...tail.matchAll(/\(#(\d+)\): connection closed/g)].map((match) => match[1]),
    );
    const open = openedIds.filter((id) => !closedIds.has(id));
    const record = {
      hubDir: process.env.EVENTS_MCP_HUB_DIR,
      sessions,
      hubPids: pids,
      reusedHub: reused,
      opened: openedIds.length,
      concurrent: open.length,
      openConnections: open,
      logPath,
    };
    write("shared-hub.json", record);
    console.log(JSON.stringify(record, null, 2));
    if (pids.length !== 1) throw new Error(`expected one hub pid, saw ${JSON.stringify(pids)}`);
    if (record.concurrent < count)
      throw new Error(`expected ${count} sessions open at once, saw ${record.concurrent}`);
    if (sessions.some((session) => session.events !== 23))
      throw new Error("a session did not list 23 events");
  } finally {
    await Promise.all(clients.map((entry) => entry.client.close()));
  }
};

const flag = (name, fallback) => {
  const index = process.argv.indexOf(`--${name}`);
  return index === -1 ? fallback : process.argv[index + 1];
};

const mode = process.argv[2] ?? "list";
if (mode === "list") await list();
else if (mode === "read-tools") await readTools();
else if (mode === "watch")
  await watch({
    timeout: flag("timeout", "900"),
    trigger: process.argv.includes("--trigger"),
  });
else if (mode === "hub") await hub({ sessions: flag("sessions", "2") });
else {
  console.error(
    "usage: mcp-client.mjs list | read-tools | watch [--timeout SECONDS] [--trigger] | hub [--sessions N]",
  );
  process.exit(2);
}
