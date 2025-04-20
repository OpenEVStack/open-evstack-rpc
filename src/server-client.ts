import { RpcClient } from "./client";
import { OPEN, WebSocket } from "ws";
import {
  ClientOptions,
  ServerClientOptions,
} from "./interfaces/Options.interface";
import { IncomingMessage } from "http";

export class RPCServerClient extends RpcClient {
  private _session: Record<string, any> | undefined;
  private _handshake: Record<string, any> | undefined;

  constructor(
    options: ClientOptions,
    { ws, handshake, session }: ServerClientOptions
  ) {
    super(options);

    this._session = session;
    this._handshake = handshake;
    this._state = OPEN;
    this._identity = options.identity;
    this._ws = ws;
    this._protocol = ws.protocol;

    this._attachWebsocket(this._ws);
  }

  public override _attachWebsocket(ws: WebSocket): void {
    this._ws = ws;

    ws.on("close", () => {
      this._state = 3; // CLOSED
    });

    // etc.
  }

  get handshake() {
    return this._handshake;
  }

  get session() {
    return this._session;
  }

  override async connect(): Promise<
    { response: IncomingMessage | undefined } | undefined
  > {
    throw new Error("Cannot connect from server to client");
  }
}
