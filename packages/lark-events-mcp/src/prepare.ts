import { CodeMode, Tool } from "@opencode/codemode";
import { Effect, Option, Schema } from "effect";
import { bytes } from "./transform.ts";

const QueryValue = Schema.Union([Schema.String, Schema.Finite, Schema.Boolean]);
export const GetInput = Schema.Struct({
  path: Schema.String,
  query: Schema.optionalKey(Schema.Record(Schema.String, QueryValue)),
});
const Envelope = Schema.Struct({
  code: Schema.Number,
  msg: Schema.String,
  data: Schema.optionalKey(Schema.Json),
});
const ErrorResponse = Schema.Struct({
  response: Schema.Struct({ data: Schema.Struct({ code: Schema.Number, msg: Schema.String }) }),
});
const HttpErrorResponse = Schema.Struct({ response: Schema.Struct({ status: Schema.Number }) });

export type LarkRequester = (input: typeof GetInput.Type, signal: AbortSignal) => Promise<unknown>;

const validPath = (path: string) =>
  path.startsWith("/") && !path.includes("//") && !path.includes("..") && !/[?#\\%]/.test(path);

export const getLark = (request: LarkRequester, { path, query }: typeof GetInput.Type) =>
  Effect.gen(function* () {
    if (!validPath(path)) return yield* Effect.fail("Invalid Lark GET path");
    const response = yield* Effect.tryPromise({
      try: (signal) => request({ path, ...(query === undefined ? {} : { query }) }, signal),
      catch: (error) => {
        const response = Schema.decodeUnknownOption(ErrorResponse)(error);
        if (Option.isSome(response))
          return `Lark API ${response.value.response.data.code}: ${response.value.response.data.msg}`;
        const http = Schema.decodeUnknownOption(HttpErrorResponse)(error);
        if (Option.isSome(http))
          return `Lark HTTP ${http.value.response.status} on ${path}; check the path`;
        return String(error);
      },
    });
    const envelope = yield* Schema.decodeUnknownEffect(Envelope)(response).pipe(
      Effect.mapError(() => "Invalid Lark response"),
    );
    if (envelope.code !== 0)
      return yield* Effect.fail(`Lark API ${envelope.code}: ${envelope.msg}`);
    return envelope.data ?? null;
  });
export const getDescription =
  "Read-only GET to the Lark OpenAPI as this app. path is relative to /open-apis, for example /im/v1/messages. Returns the response data, or fails with Lark's code and message, for example when the app lacks a scope.";
export const makePrepare = (request: LarkRequester) => {
  const get = Tool.make({
    description: getDescription,
    input: GetInput,
    output: Schema.Json,
    execute: (input) => getLark(request, input),
  });
  const runtime = CodeMode.make({
    tools: { "lark-events": { get } },
    limits: { maxOutputBytes: 65536 },
  });
  return (source: string, event: unknown) =>
    Effect.gen(function* () {
      const encoded = yield* Effect.try(() => JSON.stringify({ event })).pipe(
        Effect.mapError(() => "InvalidInput"),
      );
      if (bytes(encoded) > 1048576) return yield* Effect.fail("InputTooLarge");
      const input = yield* Schema.decodeUnknownEffect(Schema.Json)(JSON.parse(encoded)).pipe(
        Effect.mapError(() => "InvalidInput"),
      );
      const result = yield* runtime.execute(source, { input });
      if (!result.ok) return yield* Effect.fail(`${result.error.kind}: ${result.error.message}`);
      if (result.truncated) return yield* Effect.fail("OutputTooLarge");
      return yield* Schema.decodeUnknownEffect(Schema.Json)(result.value).pipe(
        Effect.mapError(() => "InvalidOutput"),
      );
    });
};
