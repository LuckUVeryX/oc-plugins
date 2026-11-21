import { Effect, Option, Schema } from "effect";
import { GetInput } from "../get.ts";
import { SourceError, type Tool } from "../source.ts";
import { makeTool } from "../tool.ts";

const Envelope = Schema.Struct({
  code: Schema.Number,
  msg: Schema.String,
  data: Schema.optionalKey(Schema.Json),
});
const ErrorResponse = Schema.Struct({
  response: Schema.Struct({ data: Schema.Struct({ code: Schema.Number, msg: Schema.String }) }),
});
const HttpErrorResponse = Schema.Struct({ response: Schema.Struct({ status: Schema.Number }) });

export type LarkRequester = (input: GetInput, signal: AbortSignal) => Promise<unknown>;

const validPath = (path: string) =>
  path.startsWith("/") && !path.includes("//") && !path.includes("..") && !/[?#\\%]/.test(path);

export const getLark = Effect.fn("Lark.get")(function* (
  request: LarkRequester,
  { path, query }: GetInput,
) {
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
  if (envelope.code !== 0) return yield* Effect.fail(`Lark API ${envelope.code}: ${envelope.msg}`);
  return envelope.data ?? null;
});

export const makeGetTool = (request: Effect.Effect<LarkRequester, SourceError>): Tool =>
  makeTool({
    description: "Read-only GET to the Lark OpenAPI as the app. path is under /open-apis.",
    input: GetInput,
    run: (input) =>
      Effect.gen(function* () {
        const requester = yield* request.pipe(Effect.mapError((error) => error.message));
        return yield* getLark(requester, input);
      }),
  });
