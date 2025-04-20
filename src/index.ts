// index.ts
import RpcClient from "./client";
import { RpcServer } from "./server";
import { createRPCError } from "./utils/rpcError";
import { createValidator } from "./utils/validator";
import * as errors from "./errors";
import * as symbols from "./utils/symbols";

export default {
  RpcServer,
  RpcClient,
  createRPCError,
  createValidator,
  ...errors,
  ...symbols,
};
