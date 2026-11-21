import { OpenCode } from "@opencode/client/effect";
import { Service as OpenCodeService } from "@opencode/client/service";
import { Generate } from "@opencode/schema/generate";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";
import { decisionModel } from "./config.ts";

export const DecideInput = Schema.Struct({
  questions: Generate.DecisionRequest.fields.questions,
  input: Schema.optionalKey(Generate.DecisionRequest.fields.input),
});
export type DecideInput = typeof DecideInput.Type;
type DecisionModel = typeof Generate.DecisionRequest.fields.model.Type;

export class Failed extends Schema.TaggedError<Failed>()("DecisionModel.Failed", {
  message: Schema.String,
}) {}

export type Decide = (input: DecideInput) => Effect.Effect<Schema.Json, Failed>;

const errorMessage = (error: { readonly cause?: unknown }): Effect.Effect<never, Failed> => {
  const cause = error.cause;
  if (!HttpClientError.isHttpClientError(cause))
    return Effect.fail(
      new Failed({ message: cause instanceof Error ? cause.message : String(cause ?? error) }),
    );
  const response = cause.response;
  if (!response) return Effect.fail(new Failed({ message: cause.message }));
  return response.text.pipe(
    Effect.catch(() => Effect.succeed("")),
    Effect.flatMap((body) =>
      Effect.fail(new Failed({ message: `${cause.message}${body ? `: ${body}` : ""}` })),
    ),
  );
};

export const makeDecisions = (
  model: DecisionModel,
  discover = OpenCodeService.discover,
): Decide => {
  const request = Effect.fn("DecisionModel.decide")(function* ({ questions, input }: DecideInput) {
    const endpoint = yield* Effect.tryPromise({
      try: () => discover(),
      catch: (error) => new Failed({ message: String(error) }),
    });
    if (!endpoint) return yield* Effect.fail(new Failed({ message: "RequestFailed" }));
    const headers = OpenCodeService.headers(
      endpoint.auth ? { url: endpoint.url, auth: endpoint.auth } : { url: endpoint.url },
    );
    const http = yield* HttpClient.HttpClient;
    const auth = http.pipe(HttpClient.mapRequest(HttpClientRequest.setHeaders(headers ?? {})));
    const client = yield* OpenCode.make({ baseUrl: endpoint.url }).pipe(
      Effect.provideService(HttpClient.HttpClient, auth),
    );
    const result = yield* client.experimental.generate
      .decisions({ model, questions, input: input ?? [] })
      .pipe(Effect.catch(errorMessage));
    return yield* Schema.decodeUnknownEffect(Schema.Json)(result).pipe(
      Effect.mapError(() => new Failed({ message: "InvalidOutput" })),
    );
  });
  return (input: DecideInput) => request(input).pipe(Effect.provide(FetchHttpClient.layer));
};

export interface Interface {
  readonly decide: Option.Option<Decide>;
}

export class Service extends Context.Service<Service, Interface>()("@events-mcp/DecisionModel") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const model = yield* decisionModel;
    return Service.of({ decide: Option.map(model, (value) => makeDecisions(value)) });
  }),
);

export * as DecisionModel from "./decision-model.ts";
