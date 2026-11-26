import { readFileSync } from "node:fs";
import { Schema } from "effect";

const Manifest = Schema.Struct({ version: Schema.String });

export const version = Schema.decodeUnknownSync(Manifest)(
  JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")),
).version;

// Compares release numbers only. A prerelease counts as its release.
export const isNewer = (a: string, b: string): boolean => {
  const [x, y] = [a, b].map((v) => v.split("-")[0]!.split(".").map(Number));
  for (let i = 0; i < 3; i++) if (x![i] !== y![i]) return x![i]! > y![i]!;
  return false;
};
