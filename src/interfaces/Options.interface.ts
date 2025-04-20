import { WebSocket } from "ws";
import { Validator } from "../utils/validator";
import { Protocol } from "../enums/protocol.enum";

export interface ClientOptions {
  identity?: string;
  endpoint?: string;
  password?: string;
  query?: string;
  callTimeoutMs: number;
  pingIntervalMs: number;
  deferPingsOnActivity: boolean;
  wsOpts?: WsOpts | {};
  headers?: {};
  protocols: string[];
  reconnect?: boolean;
  maxReconnects?: number;
  respondWithDetailedErrors?: boolean;
  callConcurrency?: number;
  maxBadMessages?: number;
  strictMode?: boolean;
  strictModeValidators: Validator[];
  backoff?: BackOff;
}

export interface BackOff {
  initalDelay: number;
  maxDelay: number;
  factor: number;
  randomisationFactor: number;
}

export interface WsOpts {
  noDelay: boolean;
  signal: any;
  headers: Record<string, string>;
}

export interface ServerOptions {
  wssOptions: WsOpts | {};
  protocols: Protocol[];
  callTimeoutMs: number;
  pingIntervalMs: number;
  deferPingsOnActivity: boolean;
  respondWithDetailedErrors: boolean;
  callConcurrency: number;
  maxBadMessages: number;
  strictMode: boolean;
  strictModeValidators: Validator[];
}

export interface IClose {
  code: number;
  reason: string;
  awaitPending?: boolean;
  force?: boolean;
  options?: ICloseOptions;
}

export interface ICloseOptions {}

export interface PendingUpgrade {
  session?: Record<string, any>;
  protocol?: string;
  handshake?: any;
}

export interface ServerClientOptions {
  ws: WebSocket;
  session?: Record<string, any>;
  handshake?: any;
}
