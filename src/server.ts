import EventEmitter, { once } from "events";
import { CLOSED, CLOSING, OPEN, WebSocket, WebSocketServer } from "ws";
import { createServer, IncomingMessage } from "http";
import { getPackageIdent } from "./utils/rpcError";
import { WebsocketUpgradeError } from "./errors";
import standardValidators from "./utils/standard-validators";
import { abortHandshake, parseSubprotocols } from "./utils/ws-utils";
import {
  IClose,
  PendingUpgrade,
  ServerOptions,
} from "./interfaces/Options.interface";
import { RPCServerClient } from "./server-client";
import { AuthCallback } from "./types/types";
import { IListenOptions, IRpcServer } from "./interfaces/server.interface";
import { Socket } from "net";

export class RpcServer extends EventEmitter implements IRpcServer {
  private _httpServerAbortControllers: Set<AbortController>;
  private _state: number;
  private _clients: Set<RPCServerClient>;
  private _pendingUpgrades: WeakMap<IncomingMessage, PendingUpgrade>;
  private _options: ServerOptions;
  private _wss: WebSocketServer;
  private _strictValidators: Map<string, any> | undefined;
  private authCallback?: AuthCallback;

  constructor(options: ServerOptions) {
    super();

    this._httpServerAbortControllers = new Set();
    this._state = OPEN;
    this._clients = new Set();
    this._pendingUpgrades = new WeakMap();

    this._options = {
      wssOptions: {},
      protocols: [],
      callTimeoutMs: 1000 * 30,
      pingIntervalMs: 1000 * 30,
      deferPingsOnActivity: false,
      respondWithDetailedErrors: false,
      callConcurrency: 1,
      maxBadMessages: Infinity,
      strictMode: false,
      strictModeValidators: [],
    };

    this.reconfigure(options || {});

    this._wss = new WebSocketServer({
      ...this._options.wssOptions,
      noServer: true,
      handleProtocols: (_, request: IncomingMessage): string => {
        const upgrade = this._pendingUpgrades.get(request);
        return upgrade?.protocol!;
      },
    });

    this._wss.on("headers", (h) => h.push(`Server: ${getPackageIdent()}`));
    this._wss.on("error", (err) => this.emit("error", err));
    this._wss.on("connection", this._onConnection.bind(this));
  }

  reconfigure(options: ServerOptions) {
    const newOpts = Object.assign({}, this._options, options);

    if (newOpts.strictMode && !newOpts.protocols?.length) {
      throw new Error(`strictMode requires at least one subprotocol`);
    }

    const strictValidators = [...standardValidators];
    if (newOpts.strictModeValidators) {
      strictValidators.push(...newOpts.strictModeValidators);
    }

    this._strictValidators = strictValidators.reduce((svs, v) => {
      svs.set(v.subprotocol, v);
      return svs;
    }, new Map<string, any>());

    let strictProtocols: string[] = [];
    if (Array.isArray(newOpts.strictMode)) {
      strictProtocols = newOpts.strictMode;
    } else if (newOpts.strictMode) {
      strictProtocols = newOpts.protocols ?? [];
    }

    const missingValidator = strictProtocols.find((protocol) =>
      this._strictValidators!.has(protocol)
    );
    if (missingValidator) {
      throw new Error(
        `Missing strictMode validator for subprotocol '${missingValidator}'`
      );
    }

    this._options = newOpts;
  }

  get handleUpgrade() {
    return async (request: IncomingMessage, socket: Socket, head: Buffer) => {
      let resolved = false;
      const ac = new AbortController();
      const { signal } = ac;

      const url = new URL("http://localhost" + (request.url || "/"));
      const pathParts = url.pathname.split("/");
      const identity = decodeURIComponent(pathParts.pop() || "");
      const endpoint = pathParts.join("/") || "/";

      const abortUpgrade = (error?: any) => {
        resolved = true;

        const code =
          error instanceof WebsocketUpgradeError &&
          error.code >= 1000 &&
          error.code <= 4999
            ? error.code
            : 1002;

        const reason =
          typeof error?.message === "string"
            ? error.message.slice(0, 123)
            : "Protocol error";

        abortHandshake(socket, code, reason);

        if (!signal.aborted) {
          ac.abort(error);
          this.emit("upgradeAborted", {
            error,
            socket,
            request,
            identity,
          });
        }
      };

      socket.on("error", (err) => {
        abortUpgrade(err);
      });

      try {
        if (this._state !== OPEN) {
          throw new WebsocketUpgradeError(1013, "Server not open");
        }

        const headers = request.headers;

        if (headers.upgrade?.toLowerCase() !== "websocket") {
          throw new WebsocketUpgradeError(
            1002,
            "Can only upgrade websocket upgrade requests"
          );
        }

        const remoteAddress = request.socket.remoteAddress;
        const protocols =
          "sec-websocket-protocol" in headers
            ? parseSubprotocols(headers["sec-websocket-protocol"]!)
            : new Set<string>();

        let password: Buffer | undefined;

        if (headers.authorization) {
          try {
            const b64up =
              headers.authorization.match(
                /^ *(?:[Bb][Aa][Ss][Ii][Cc]) +([A-Za-z0-9._~+/-]+=*) *$/
              )?.[1] ?? "";
            const userPassBuffer = Buffer.from(b64up, "base64");
            const identityBuf = Buffer.from(identity + ":");

            if (
              userPassBuffer.compare(identityBuf, 0, identityBuf.length) === 0
            ) {
              password = userPassBuffer.subarray(identityBuf.length);
            }
          } catch {
            // ignore
          }
        }

        const handshake = {
          remoteAddress,
          headers,
          protocols,
          endpoint,
          identity,
          query: url.searchParams,
          request,
          password,
        };

        const accept = (session?: Record<string, any>, protocol?: string) => {
          if (resolved) return;
          resolved = true;

          try {
            if ((socket as any).readyState !== "open") {
              throw new WebsocketUpgradeError(
                1002,
                `Client readyState = '${(socket as any).readyState}'`
              );
            }

            if (protocol === undefined) {
              protocol = (this._options.protocols ?? []).find((p) =>
                protocols.has(p)
              );
            } else if (!protocols.has(protocol)) {
              throw new WebsocketUpgradeError(
                1002,
                `Client doesn't support expected subprotocol`
              );
            }

            this._pendingUpgrades.set(request, {
              session: session ?? {},
              protocol,
              handshake,
            });

            this._wss.handleUpgrade(request, socket, head, (ws) => {
              this._wss.emit("connection", ws, request);
            });
          } catch (err) {
            abortUpgrade(err);
          }
        };

        const reject = (code = 1002, message = "Not found") => {
          if (resolved) return;
          resolved = true;
          abortUpgrade(new WebsocketUpgradeError(code, message));
        };

        socket.once("end", () => {
          reject(1002, `Client connection closed before upgrade complete`);
        });

        socket.once("close", () => {
          reject(1002, `Client connection closed before upgrade complete`);
        });

        if (this.authCallback) {
          await this.authCallback(accept, reject, handshake, signal);
        } else {
          accept();
        }
      } catch (err) {
        abortUpgrade(err);
      }
    };
  }

  async _onConnection(websocket: WebSocket, request: IncomingMessage) {
    try {
      if (this._state !== OPEN) {
        throw new Error("Server is no longer open");
      }

      const { handshake, session } = this._pendingUpgrades.get(request) ?? {};

      const client = new RPCServerClient(
        {
          identity: handshake.identity,
          reconnect: false,
          callTimeoutMs: this._options.callTimeoutMs,
          pingIntervalMs: this._options.pingIntervalMs,
          deferPingsOnActivity: this._options.deferPingsOnActivity,
          respondWithDetailedErrors: this._options.respondWithDetailedErrors,
          callConcurrency: this._options.callConcurrency,
          strictMode: this._options.strictMode,
          strictModeValidators: this._options.strictModeValidators,
          maxBadMessages: this._options.maxBadMessages,
          protocols: this._options.protocols,
        },
        {
          ws: websocket,
          session,
          handshake,
        }
      );

      this._clients.add(client);
      client.once("close", () => this._clients.delete(client));
      this.emit("client", client);
    } catch (err: any) {
      const code =
        err?.statusCode >= 1000 && err?.statusCode <= 4999
          ? err.statusCode
          : 1011;
      const reason = (err?.message || "Internal error").slice(0, 123);
      websocket.close(code, reason);
    }
  }

  auth(cb: AuthCallback) {
    this.authCallback = cb;
  }

  async listen(port: number, host: string, options: IListenOptions) {
    const ac = new AbortController();
    this._httpServerAbortControllers.add(ac);

    if (options.signal) {
      once(options.signal, "abort").then(() => {
        ac.abort(options.signal?.reason);
      });
    }

    const httpServer = createServer(
      {
        noDelay: true,
      },
      (req, res) => {
        res.setHeader("Server", getPackageIdent());
        res.statusCode = 404;
        res.end();
      }
    );

    httpServer.on("upgrade", this.handleUpgrade);
    httpServer.once("close", () => this._httpServerAbortControllers.delete(ac));

    await new Promise<void>((resolve, reject) => {
      httpServer.listen({ port, host, signal: ac.signal }, (err?: any) =>
        err ? reject(err) : resolve()
      );
    });

    return httpServer;
  }

  async close({ code, reason, awaitPending, force }: IClose) {
    if (this._state === OPEN) {
      this._state = CLOSING;
      this.emit("closing");
      const closeCode = code ?? 1001;

      await Promise.all(
        Array.from(this._clients).map((cli) =>
          cli.close({ code: closeCode, reason, awaitPending, force })
        )
      );

      await new Promise<void>((resolve, reject) => {
        this._wss.close((err?: Error) => (err ? reject(err) : resolve()));
        this._httpServerAbortControllers.forEach((ac) => ac.abort("Closing"));
      });

      this._state = CLOSED;
      this.emit("close");
    }
  }
}
