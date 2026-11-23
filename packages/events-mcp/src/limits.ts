export const bytes = (value: string): number => Buffer.byteLength(value, "utf8");

// Caps one wake from a watch's code.
export const MAX_EVENT_BYTES = 16_384;

// Caps a watch's code, its output, and its stored state.
export const MAX_CODE_BYTES = 65_536;
