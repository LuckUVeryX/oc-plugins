import { Effect, type Option, Result, type Schema, type Scope, Stream } from "effect";
import type { Event, SourceError, Feed } from "../source/contract.ts";
import { GitlabSocket, type ChannelIdentifier } from "./socket.ts";

/** Turns one socket channel's frames into a source feed, dropping frames that map to none. */
export const subscribeToChannel = (options: {
  readonly socket: GitlabSocket.Interface;
  readonly identifier: ChannelIdentifier;
  readonly toEvent: (message: Schema.Json) => Effect.Effect<Option.Option<Event>, SourceError>;
}): Effect.Effect<Feed, SourceError, Scope.Scope> =>
  Effect.gen(function* () {
    const messages = yield* options.socket.subscribe(options.identifier);
    return {
      events: messages.pipe(
        Stream.mapEffect(options.toEvent),
        Stream.filterMap((event) => Result.fromOption(event, () => undefined)),
      ),
      health: options.socket.health,
    };
  });
