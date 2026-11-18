import { OpenCode } from "@opencode/client/effect";
import { Service } from "@opencode/client/service";
import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from "effect/http";

export const Decisions = Schema.Struct({
  questions: Generate.DecisionRequest.fields.questions,
  input: Schema.optionalKey(Generate.DecisionRequest.fields.input),
}).annotate({
  description:
    "The operator's model answers your questions about prepare's value before transform runs. Read the answers at input.result.",
});
export type Decisions = typeof Decisions.Type;
export type DecisionModel = typeof Generate.DecisionRequest.fields.model.Type;

export const makeDecisions = (model: DecisionModel, discover = Service.discover) =>
  Effect.fn("LarkDecisions.request")(function* (decisions: Decisions, event: unknown) {
    const value = yield* Schema.decodeUnknownEffect(Schema.Json)(event).pipe(
      Effect.mapError(() => "InvalidInput" as const),
    );
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
    return yield* client.experimental.generate
      .decisions({
        model,
        questions: decisions.questions,
        input: [...(decisions.input ?? []), { type: "json", value }],
      })
      .pipe(
        Effect.catch((error) => {
          const cause = error.cause;
          if (HttpClientError.isHttpClientError(cause)) {
            const response = cause.response;
            if (response)
              return response.text.pipe(
                Effect.catch(() => Effect.succeed("")),
                Effect.flatMap((body) => Effect.fail(`${cause.message}${body ? `: ${body}` : ""}`)),
              );
            return Effect.fail(cause.message);
          }
          return Effect.fail(cause instanceof Error ? cause.message : String(cause ?? error));
        }),
      );
  }, Effect.provide(FetchHttpClient.layer));
