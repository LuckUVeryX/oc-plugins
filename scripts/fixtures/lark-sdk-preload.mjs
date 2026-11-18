import { mock } from "bun:test";

mock.module("@larksuiteoapi/node-sdk", () => ({
  Client: class {
    request() {
      return Promise.reject(new Error("offline SDK"));
    }
  },
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
