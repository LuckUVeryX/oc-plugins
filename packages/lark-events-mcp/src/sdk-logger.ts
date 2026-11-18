const safeMessages = new Set([
  "[ws]",
  "event-dispatch is ready",
  "ws client ready",
  "ws connect success",
  "ws connect failed",
  "connect failed",
  "reconnect",
  "reconnect success",
  "ws error",
  "client closed",
]);

const log = (level: string, message: unknown[]) =>
  console.error(
    `[sdk ${level}]`,
    ...message.map((part) =>
      typeof part === "string" && safeMessages.has(part) ? part : "[redacted]",
    ),
  );

export const logger = {
  error: (...message: unknown[]) => log("error", message),
  warn: (...message: unknown[]) => log("warn", message),
  info: (...message: unknown[]) => log("info", message),
  debug: (...message: unknown[]) => log("debug", message),
  trace: (...message: unknown[]) => log("trace", message),
};
