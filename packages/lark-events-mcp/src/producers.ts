import { Domain, EventDispatcher, WSClient } from "@larksuiteoapi/node-sdk";
import {
  Cache,
  Config,
  Context,
  Deferred,
  Effect,
  Layer,
  PubSub,
  Redacted,
  Ref,
  Schema,
} from "effect";

export class SourceError extends Schema.TaggedError<SourceError>()("SourceError", {
  message: Schema.String,
}) {}

export type SourceMessage =
  | {
      readonly kind: "event";
      readonly payload: unknown;
      readonly eventId: string;
      readonly timestamp: string;
      readonly timestampOrigin: "upstream" | "local";
    }
  | { readonly kind: "reconnecting" }
  | { readonly kind: "reconnected" }
  | { readonly kind: "terminated"; readonly message: string };

export class EventProducers extends Context.Service<
  EventProducers,
  {
    readonly acquire: (event: string) => Effect.Effect<
      {
        readonly messages: PubSub.PubSub<SourceMessage>;
        readonly ready: Effect.Effect<void, SourceError>;
        readonly terminal: Effect.Effect<string | undefined>;
      },
      SourceError
    >;
  }
>()("lark-events-mcp/EventProducers") {}

const logger = {
  error: (...message: unknown[]) => console.error(...message),
  warn: (...message: unknown[]) => console.error(...message),
  info: (...message: unknown[]) => console.error(...message),
  debug: (...message: unknown[]) => console.error(...message),
  trace: (...message: unknown[]) => console.error(...message),
};

const metadata = (payload: unknown) => {
  const object = typeof payload === "object" && payload !== null ? payload : {};
  const id = "event_id" in object ? object.event_id : undefined;
  const time = "create_time" in object ? object.create_time : undefined;
  const date = typeof time === "string" && /^\d+$/.test(time) ? new Date(Number(time)) : undefined;
  const upstreamTime = date && !Number.isNaN(date.getTime()) ? date.toISOString() : undefined;
  return {
    upstreamId: typeof id === "string" && id ? id : undefined,
    eventId: typeof id === "string" && id ? id : `local-${crypto.randomUUID()}`,
    timestamp: upstreamTime ?? new Date().toISOString(),
    timestampOrigin: upstreamTime ? ("upstream" as const) : ("local" as const),
  };
};

export const producersLayer = Layer.effect(
  EventProducers,
  Effect.gen(function* () {
    const appId = yield* Config.schema(
      Schema.String.check(Schema.isPattern(/^cli_[0-9a-fA-F]{16}$/)),
      "LARK_APP_ID",
    );
    const secret = yield* Config.schema(
      Schema.RedactedFromValue(Schema.NonEmptyString),
      "LARK_APP_SECRET",
    );
    let ready = yield* Deferred.make<void, SourceError>();
    const seen = yield* Cache.make({
      capacity: 10_000,
      timeToLive: "1 hour",
      lookup: (_key: string) => Ref.make(false),
    });
    const dispatcher = new EventDispatcher({ logger });
    const entries = new Map<string, PubSub.PubSub<SourceMessage>>();
    let terminal: string | undefined;
    const publish = (message: SourceMessage) => {
      for (const messages of entries.values()) Effect.runFork(PubSub.publish(messages, message));
    };
    const fail = (error: SourceError) => {
      terminal = error.message;
      Effect.runFork(Deferred.fail(ready, error));
      publish({ kind: "terminated", message: error.message });
    };
    const client = new WSClient({
      appId,
      appSecret: Redacted.value(secret),
      domain: Domain.Lark,
      logger,
      onReady: () => {
        Effect.runFork(Deferred.succeed(ready, undefined));
      },
      onReconnecting: () => {
        ready = Effect.runSync(Deferred.make<void, SourceError>());
        publish({ kind: "reconnecting" });
      },
      onReconnected: () => {
        Effect.runFork(Deferred.succeed(ready, undefined));
        publish({ kind: "reconnected" });
      },
      onError: (error) => fail(new SourceError({ message: error.message })),
    });
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Effect.sync(() => client.close({ force: true }));
        yield* Effect.forEach(entries.values(), PubSub.shutdown, { discard: true });
      }),
    );
    void client.start({ eventDispatcher: dispatcher }).catch((error: unknown) => {
      fail(new SourceError({ message: error instanceof Error ? error.message : String(error) }));
    });
    const acquire = Effect.fn("EventProducers.acquire")(function* (event: string) {
      if (terminal !== undefined) return yield* Effect.fail(new SourceError({ message: terminal }));
      let messages = entries.get(event);
      if (!messages) {
        messages = yield* PubSub.unbounded<SourceMessage>();
        entries.set(event, messages);
        const channel = messages;
        dispatcher.register({
          [event]: (payload: unknown) => {
            const { upstreamId, ...details } = metadata(payload);
            Effect.runFork(
              Effect.gen(function* () {
                if (upstreamId) {
                  const admitted = yield* Cache.get(seen, JSON.stringify([event, upstreamId]));
                  if (yield* Ref.getAndSet(admitted, true)) return;
                }
                yield* PubSub.publish(channel, { kind: "event", payload, ...details });
              }),
            );
          },
        });
      }
      return {
        messages,
        ready: Effect.suspend(() => Deferred.await(ready)),
        terminal: Effect.sync(() => terminal),
      };
    });
    return EventProducers.of({ acquire });
  }),
);
