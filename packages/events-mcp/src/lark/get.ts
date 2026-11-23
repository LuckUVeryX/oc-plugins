import { Effect, Option, Schema } from "effect";
import { ToolError, type Tool } from "../source/contract.ts";
import { GetInput, makeTool } from "../source/tool.ts";
import type { LarkAuth } from "./auth.ts";

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
  input: GetInput,
): Effect.fn.Return<Schema.Json, ToolError> {
  if (!validPath(input.path))
    return yield* Effect.fail(new ToolError({ message: "Invalid Lark GET path" }));
  const response = yield* Effect.tryPromise({
    try: (signal) => request(input, signal),
    catch: (error) => {
      const response = Schema.decodeUnknownOption(ErrorResponse)(error);
      if (Option.isSome(response))
        return new ToolError({
          message: `Lark API ${response.value.response.data.code}: ${response.value.response.data.msg}`,
        });
      const http = Schema.decodeUnknownOption(HttpErrorResponse)(error);
      if (Option.isSome(http))
        return new ToolError({
          message: `Lark HTTP ${http.value.response.status} on ${input.path}; check the path`,
        });
      return new ToolError({ message: String(error) });
    },
  });
  const envelope = yield* Schema.decodeUnknownEffect(Envelope)(response).pipe(
    Effect.mapError(() => new ToolError({ message: "Invalid Lark response" })),
  );
  if (envelope.code !== 0)
    return yield* Effect.fail(
      new ToolError({ message: `Lark API ${envelope.code}: ${envelope.msg}` }),
    );
  return envelope.data ?? null;
});

export const makeGetTool = (api: LarkAuth.Interface): Tool =>
  makeTool({
    description:
      "Read Lark data with the app's permissions, for example to resolve a chat_id or read a message before arming a watch or from inside the script. path starts with /, stays under /open-apis, and carries no query string or percent escapes. Pass parameters in query and page with page_token. Returns the response's data. Lark API errors include the API code and message.",
    input: GetInput,
    run: (input) =>
      Effect.gen(function* () {
        const credentials = yield* api
          .credentials()
          .pipe(Effect.mapError((error) => new ToolError({ message: error.message })));
        return yield* getLark(credentials.request, input);
      }),
  });
