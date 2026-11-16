import { Generate } from "@opencode/schema/generate";
import { Schema } from "effect";

export const Root = Schema.Struct({
  chatId: Schema.String,
  rootId: Schema.String,
});
export interface Root extends Schema.Schema.Type<typeof Root> {}

export const LarkEvent = Schema.Struct({
  type: Schema.Literal("im.message.receive_v1"),
  chat_id: Schema.String,
  message_id: Schema.String,
  content: Schema.String,
  chat_type: Schema.optionalKey(Schema.Literals(["p2p", "group"])),
  create_time: Schema.optionalKey(Schema.String),
  event_id: Schema.optionalKey(Schema.String),
  id: Schema.optionalKey(Schema.String),
  mentions: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        id: Schema.optionalKey(Schema.String),
        key: Schema.optionalKey(Schema.String),
        name: Schema.optionalKey(Schema.String),
      }),
    ),
  ),
  message_type: Schema.optionalKey(Schema.String),
  reply_to: Schema.optionalKey(Schema.String),
  root_id: Schema.optionalKey(Schema.String),
  sender_id: Schema.optionalKey(Schema.String),
  sender_type: Schema.optionalKey(Schema.Literals(["user", "bot"])),
  thread_id: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
  update_time: Schema.optionalKey(Schema.String),
});
export interface LarkEvent extends Schema.Schema.Type<typeof LarkEvent> {}

export const HistoryMessage = Schema.Struct({
  message_id: Schema.String,
  content: Schema.String,
  msg_type: Schema.optionalKey(Schema.String),
  create_time: Schema.optionalKey(Schema.String),
  update_time: Schema.optionalKey(Schema.String),
  root_id: Schema.optionalKey(Schema.String),
  parent_id: Schema.optionalKey(Schema.String),
  thread_id: Schema.optionalKey(Schema.String),
  chat_id: Schema.optionalKey(Schema.String),
  sender: Schema.optionalKey(Schema.Json),
  mentions: Schema.optionalKey(Schema.Array(Schema.Json)),
  deleted: Schema.optionalKey(Schema.Boolean),
  updated: Schema.optionalKey(Schema.Boolean),
});
export interface HistoryMessage extends Schema.Schema.Type<typeof HistoryMessage> {}

export const History = Schema.Struct({
  items: Schema.Array(HistoryMessage),
  hasMore: Schema.Boolean,
});
export interface History extends Schema.Schema.Type<typeof History> {}

export const ConversationContext = Schema.Struct({
  root: Root,
  events: Schema.Array(LarkEvent),
  history: History,
});
export interface ConversationContext extends Schema.Schema.Type<typeof ConversationContext> {}

export const TransformInput = Schema.Struct({
  ...ConversationContext.fields,
  result: Generate.DecisionResult,
});
export interface TransformInput extends Schema.Schema.Type<typeof TransformInput> {}

export type Transform = (input: TransformInput) => string | null | Promise<string | null>;
