export const bytes = (value: string): number => Buffer.byteLength(value, "utf8");

// OpenCode fails a delivered { id, timestamp, data } above 16 KB, so mirror its exact cap.
export const MAX_EVENT_BYTES = 16_384;

export const MAX_SCRIPT_BYTES = 65_536;
export const MAX_SCRIPT_OUTPUT_BYTES = 65_536;
export const MAX_STATE_BYTES = 65_536;
