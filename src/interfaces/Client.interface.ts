import EventBuffer from "../utils/event-buffer";
import WebSocket from "ws";
import { ClientOptions, IClose } from "./Options.interface";
import { IncomingMessage } from "http";

export interface IRpcClient {
  reconfigure(options: ClientOptions): void;
  connect(): Promise<{ response: IncomingMessage | undefined } | undefined>;
  sendRaw(message: string): void;
  close(payload: IClose): Promise<void>;
  handle(method: FunctionConstructor, handler: any): void;
  removeHandler(method?: string): void;
  removeAllHandlers(): void;
  call(method: string, params: string, options: ICallOptions): Promise<void>;
  _onCall(msgId: string, method: string, params: string): void;
  _attachWebsocket(ws: WebSocket, leadMsgBuffer: EventBuffer): void;
  _rejectPendingCalls(abortReason: string): void;
  _awaitUntilPendingSettled(): Promise<PromiseSettledResult<any>[]>;
  _handleDisconnect(payload: IDisconnect): void;
  _beginConnect(): void;
  _onMessage(buffer: string | ArrayBuffer | Buffer[]): void;
  _onCall(msgId: string, method: string, params: string): Promise<void>;
  _onCallResult(msgId: string, result: string): void;
  _onCallError(
    msgId: string,
    errorCode: number,
    errorDescription: string,
    errorDetails: Record<string, any> | undefined
  ): void;
}

export interface ICallOptions {
  callTimeoutMs?: number;
  noReply: boolean;
  signal: any;
}


export interface IPendingCall {
  resolve?: (...args: any[]) => void;
  msgId: string;
  method: string;
  params?: string;
  timeout?: Promise<void>;
  reject?: any;
  promise?: any;
  abort?: (reason: string) => void;
}

export interface IDisconnect {
  code: number;
  reason: Buffer | string;
}
