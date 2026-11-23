export const bytes = (value: string): number => Buffer.byteLength(value, "utf8");

// OpenCode fails a delivered { id, timestamp, data } above 16 KB, so mirror its exact cap.
export const MAX_EVENT_BYTES = 16_384;

// Caps a watch's code, its output, and its stored state.
export const MAX_CODE_BYTES = 65_536;
