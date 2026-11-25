import {
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
  PubSub,
  RcRef,
  Schema,
  type Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { io } from "socket.io-client";
import { Health, Rejected, type SourceError } from "../source/contract.ts";
import { token } from "./client.ts";

// Most updates patch a step and carry no status.
const BuildUpdate = Schema.Struct({
  buildId: Schema.String,
  context: Schema.Struct({ appId: Schema.String }),
  payload: Schema.Struct({ status: Schema.optionalKey(Schema.String) }),
});
export type BuildUpdate = typeof BuildUpdate.Type;

export interface Session {
  readonly updates: Stream.Stream<BuildUpdate, SourceError>;
  /** Emits Live once per connection. */
  readonly health: Stream.Stream<Health>;
}

export interface Interface {
  readonly subscribe: Effect.Effect<Session, SourceError, Scope.Scope>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/CodemagicSocket") {}

const rejected = new Rejected({
  message: "Codemagic refused the connection. Check CODEMAGIC_TOKEN, then arm a new watch.",
});

export const makeSocket = (options: {
  readonly url: string;
  readonly token: Effect.Effect<string, SourceError>;
}): Effect.Effect<Interface, never, Scope.Scope> =>
  Effect.gen(function* () {
    const connection = yield* RcRef.make({
      acquire: Effect.gen(function* () {
        const scope = yield* Effect.scope;
        const token = yield* options.token;
        const health = yield* SubscriptionRef.make<Health>(
          Health.Degraded({ reason: "connecting" }),
        );
        const updates = yield* PubSub.unbounded<BuildUpdate>();
        const settled = yield* Deferred.make<void, SourceError>();
        const refused = yield* Deferred.make<never, SourceError>();
        const socket = yield* Effect.acquireRelease(
          Effect.sync(() =>
            io(options.url, {
              forceNew: true,
              transports: ["websocket"],
              extraHeaders: { "x-auth-token": token },
            }),
          ),
          (socket) => Effect.sync(() => socket.disconnect()),
        );
        const setHealth = (next: Health) => Effect.runFork(SubscriptionRef.set(health, next));
        const settle = () => Effect.runFork(Deferred.succeed(settled, undefined));
        socket.on("connect", () => {
          setHealth(Health.Live());
          settle();
        });
        socket.on("disconnect", (reason) => {
          setHealth(Health.Degraded({ reason: `Codemagic connection closed: ${reason}` }));
          // socket.io-client reconnects on its own except after the server closes the connection.
          if (reason === "io server disconnect")
            Effect.runFork(
              Effect.sleep("1 second").pipe(
                Effect.andThen(Effect.sync(() => socket.connect())),
                Effect.forkIn(scope),
              ),
            );
        });
        socket.on("connect_error", (error) => {
          setHealth(Health.Degraded({ reason: `Codemagic connection failed: ${error.message}` }));
          // An inactive socket was refused by the server and will not retry.
          if (socket.active) return settle();
          Effect.runFork(Deferred.fail(settled, rejected));
          Effect.runFork(Deferred.fail(refused, rejected));
        });
        socket.on("BUILD_UPDATE", (message: unknown) => {
          const update = Schema.decodeUnknownOption(BuildUpdate)(message);
          Effect.runFork(
            Option.isSome(update)
              ? PubSub.publish(updates, update.value)
              : Effect.logWarning("Ignoring a malformed Codemagic BUILD_UPDATE"),
          );
        });
        yield* Deferred.await(settled);
        return { updates, health, refused };
      }),
    });
    return {
      subscribe: Effect.gen(function* () {
        const { updates, health, refused } = yield* RcRef.get(connection);
        const subscription = yield* PubSub.subscribe(updates);
        return {
          updates: Stream.fromSubscription(subscription).pipe(
            Stream.merge(Stream.fromEffect(Deferred.await(refused))),
          ),
          health: SubscriptionRef.changes(health),
        };
      }),
    };
  });

export const layer = Layer.effect(
  Service,
  makeSocket({ url: "wss://ws2.codemagic.io", token }).pipe(Effect.map(Service.of)),
);

export * as CodemagicSocket from "./socket.ts";
