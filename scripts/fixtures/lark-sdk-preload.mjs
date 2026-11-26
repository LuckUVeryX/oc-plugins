import { registerHooks } from "node:module";

const offline = `
export class Client {
  request() {
    return Promise.reject(new Error("offline SDK"));
  }
}
export const Domain = { Lark: 1 };
export class EventDispatcher {
  register() {}
}
export class WSClient {
  start() {
    console.error("offline SDK started");
    return Promise.resolve();
  }
  close() {}
}
`;

registerHooks({
  resolve: (specifier, context, next) =>
    specifier === "@larksuiteoapi/node-sdk"
      ? { url: `data:text/javascript,${encodeURIComponent(offline)}`, shortCircuit: true }
      : next(specifier, context),
});
