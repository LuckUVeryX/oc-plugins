import { Effect, Logger } from "effect";

const write = (log: Effect.Effect<void>): void => {
  Effect.runSync(log.pipe(Effect.provideService(Logger.LogToStderr, true)));
};

export const sdkLogger = {
  error: (...message: ReadonlyArray<unknown>) => write(Effect.logError(...message)),
  warn: (...message: ReadonlyArray<unknown>) => write(Effect.logWarning(...message)),
  info: (...message: ReadonlyArray<unknown>) => write(Effect.logInfo(...message)),
  debug: (...message: ReadonlyArray<unknown>) => write(Effect.logDebug(...message)),
  trace: (...message: ReadonlyArray<unknown>) => write(Effect.logTrace(...message)),
};
