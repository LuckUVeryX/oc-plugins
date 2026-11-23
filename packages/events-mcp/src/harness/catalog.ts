import type { Harness } from "./harness.ts";
import { harness as claude } from "./claude.ts";
import { harness as codex } from "./codex.ts";
import { harness as opencode } from "./opencode.ts";

export const harnesses = { opencode, claude, codex } satisfies Record<string, Harness>;
export type Name = keyof typeof harnesses;

export const isName = (value: string): value is Name => Object.hasOwn(harnesses, value);

export const names = Object.keys(harnesses).filter(isName);

export const get = (name: Name): Harness => harnesses[name];
