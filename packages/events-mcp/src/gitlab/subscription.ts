import { Effect, type Option, Result, type Schema, type Scope, Stream } from "effect";
import type { Occurrence, SourceError, Subscription } from "../source.ts";
import type { Cable, CableIdentifier } from "./cable.ts";

/** Turns one cable channel's frames into a source subscription, dropping frames that map to none. */
export const subscribeToChannel = (options: {
  readonly cable: Effect.Effect<Cable, SourceError, Scope.Scope>;
  readonly identifier: CableIdentifier;
  readonly toOccurrence: (
    message: Schema.Json,
  ) => Effect.Effect<Option.Option<Occurrence>, SourceError>;
}): Effect.Effect<Subscription, SourceError, Scope.Scope> =>
  Effect.gen(function* () {
    const cable = yield* options.cable;
    const messages = yield* cable.subscribeUnique(options.identifier);
    return {
      occurrences: messages.pipe(
        Stream.mapEffect(options.toOccurrence),
        Stream.filterMap((occurrence) => Result.fromOption(occurrence, () => undefined)),
      ),
      health: cable.health,
    };
  });
