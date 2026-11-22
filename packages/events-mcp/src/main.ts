#!/usr/bin/env bun
import { isName, names } from "./harness/index.ts";

const name = process.argv[2];
if (name === undefined) {
  console.error(`events-mcp: no harness given. Use one of: ${names.join(", ")}.`);
  process.exit(1);
} else if (name === "hub") {
  const { main } = await import("./hub/serve.ts");
  main();
} else if (isName(name)) {
  const { main } = await import("./hub/proxy.ts");
  await main(name);
} else {
  console.error(`events-mcp: unknown harness "${name}". Use one of: ${names.join(", ")}.`);
  process.exit(1);
}
