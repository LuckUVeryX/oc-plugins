import type { Harness } from "../core/harness.ts";
import { harness as opencode } from "./opencode.ts";

export const names = ["opencode"] as const;
export type Name = (typeof names)[number];

export const isName = (value: string): value is Name =>
  (names as ReadonlyArray<string>).includes(value);

export const get = (name: Name): Harness => {
  switch (name) {
    case "opencode":
      return opencode;
  }
};
