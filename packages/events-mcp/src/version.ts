import { readFileSync } from "node:fs";
import { Schema } from "effect";

const Manifest = Schema.Struct({ version: Schema.String });

export const version = Schema.decodeUnknownSync(Manifest)(
  JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")),
).version;
