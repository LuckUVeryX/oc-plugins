import { OpenCode } from "@opencode/client/effect";
import { Service } from "@opencode/client/service";
import { Generate } from "@opencode/schema/generate";
import { Effect, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";

export const Decisions = Schema.Struct({
  questions: Generate.DecisionRequest.fields.questions,
  input: Schema.optionalKey(Generate.DecisionRequest.fields.input),
}).annotate({ description: "Native questions and optional standing input. Requires transform." });
export type Decisions = typeof Decisions.Type;
export type DecisionModel = typeof Generate.DecisionRequest.fields.model.Type;

type Endpoint = NonNullable<Awaited<ReturnType<typeof Service.discover>>>;

export const makeDecisions =
  (model: DecisionModel, discover: () => Promise<Endpoint | undefined> = Service.discover) =>
  (decisions: Decisions, event: Schema.Json) =>
    Effect.gen(function* () {
      const endpoint = yield* Effect.tryPromise(() => discover());
      if (!endpoint) return yield* Effect.fail("ServiceUnavailable" as const);
      const headers = Service.headers(
        endpoint.auth ? { url: endpoint.url, auth: endpoint.auth } : { url: endpoint.url },
      );
      const http = yield* HttpClient.HttpClient;
      const auth = http.pipe(HttpClient.mapRequest(HttpClientRequest.setHeaders(headers ?? {})));
      const client = yield* OpenCode.make({ baseUrl: endpoint.url }).pipe(
        Effect.provideService(HttpClient.HttpClient, auth),
      );
      return yield* client.experimental.generate.decisions({
        model,
        questions: decisions.questions,
        input: [...(decisions.input ?? []), { type: "json", value: event }],
      });
    }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.mapError(() => "RequestFailed" as const),
    );
