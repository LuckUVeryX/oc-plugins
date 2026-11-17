import { mock } from "bun:test";

mock.module("@larksuiteoapi/node-sdk", () => ({
  Domain: { Lark: 1 },
  EventDispatcher: class {
    register() {}
  },
  WSClient: class {
    start() {
      console.error("offline SDK started");
      return Promise.resolve();
    }
    close() {}
  },
}));
