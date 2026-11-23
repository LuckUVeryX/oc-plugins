import { Effect } from "effect";

export const text = (value: unknown) => ({
  content: [
    { type: "text" as const, text: typeof value === "string" ? value : JSON.stringify(value) },
  ],
});

/** Answers a tool call with the effect's value, or with its error message marked as an error. */
export const toolResult = <A, E extends { readonly message: string }>(
  effect: Effect.Effect<A, E>,
) =>
  Effect.match(effect, {
    onFailure: (error) => ({ ...text(error.message), isError: true }),
    onSuccess: text,
  });
