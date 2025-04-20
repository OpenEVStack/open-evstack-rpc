import { WebSocket } from "ws";
import { IClose, ServerOptions } from "./Options.interface";
import { IncomingMessage, Server, ServerResponse } from "http";

export interface IRpcServer {
  reconfigure(options: ServerOptions): void;
  _onConnection(websocket: WebSocket, request: IncomingMessage): void;
  listen(
    port: number,
    host: string,
    options: IListenOptions
  ): Promise<Server<typeof IncomingMessage, typeof ServerResponse>>;
  close(payload: IClose): Promise<void>;
}

export interface IListenOptions {
  signal?: AbortSignal;
}
