import { OpenCode } from "@opencode/client/effect";
import { Service } from "@opencode/client/service";
import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";

export const DecideInput = Schema.Struct({
  questions: Generate.DecisionRequest.fields.questions,
  input: Schema.optionalKey(Generate.DecisionRequest.fields.input),
});
export type DecideInput = typeof DecideInput.Type;
type DecisionModel = typeof Generate.DecisionRequest.fields.model.Type;

const errorMessage = (error: { readonly cause?: unknown }): Effect.Effect<never, string> => {
  const cause = error.cause;
  if (!HttpClientError.isHttpClientError(cause))
    return Effect.fail(cause instanceof Error ? cause.message : String(cause ?? error));
  const response = cause.response;
  if (!response) return Effect.fail(cause.message);
  return response.text.pipe(
    Effect.catch(() => Effect.succeed("")),
    Effect.flatMap((body) => Effect.fail(`${cause.message}${body ? `: ${body}` : ""}`)),
  );
};

export const makeDecisions = (model: DecisionModel, discover = Service.discover) => {
  const request = Effect.fn("EventsDecisions.decide")(function* ({
    questions,
    input,
  }: DecideInput) {
    const endpoint = yield* Effect.tryPromise({
      try: () => discover(),
      catch: (error) => String(error),
    });
    if (!endpoint) return yield* Effect.fail("RequestFailed" as const);
    const headers = Service.headers(
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
      Effect.mapError(() => "InvalidOutput" as const),
    );
  });
  return (input: DecideInput) => request(input).pipe(Effect.provide(FetchHttpClient.layer));
};
