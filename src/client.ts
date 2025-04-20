import { EventEmitter, once } from "events";
import { ClientOptions, IClose, WsOpts } from "./interfaces/Options.interface";
import { randomUUID } from "crypto";
import { setTimeout } from "timers/promises";
import WebSocket from "ws";
const { CONNECTING, OPEN, CLOSING, CLOSED } = WebSocket;
import standardValidators from "./utils/standard-validators";
import {
  createRPCError,
  getErrorPlainObject,
  getPackageIdent,
} from "./utils/rpcError";
import EventBuffer from "./utils/event-buffer";
import {
  ICallOptions,
  IDisconnect,
  IPendingCall,
  IRpcClient,
} from "./interfaces/Client.interface";
import { ExponentialStrategy } from "backoff";
import { isValidStatusCode } from "./utils/ws-utils";
import { MSG_CALL, MSG_CALLERROR, MSG_CALLRESULT } from "./utils/MSG";
import { NOREPLY } from "./utils/symbols";
import {
  RPCFrameworkError,
  RPCGenericError,
  RPCMessageTypeNotSupportedError,
  TimeoutError,
  UnexpectedHttpResponse,
} from "./errors";
import { IncomingMessage } from "http";

export class RpcClient extends EventEmitter implements IRpcClient {
  public _options: ClientOptions;
  public _identity: string | undefined;
  public _connectionUrl: string | undefined;
  public _wildcardHandler: Function | undefined;
  public _handlers: Map<string, Function>;
  public _state: number; // You can change this to an enum type like ConnectionState
  public _callQueue: any; // Replace `any` with your Queue class type

  public _ws: WebSocket | undefined;
  public _wsAbortController: any;
  public _keepAliveAbortController: any;
  public _pendingPingResponse: boolean;
  public _lastPingTime: number;
  public _closePromise: any;
  public protocol?: string;
  public _protocolOptions: string[];
  public _protocol: any;
  public _strictProtocols: string[];
  public _strictValidators: any;

  public _pendingCalls: Map<string, any>;
  public _pendingResponses: Map<string, any>;
  public _outboundMsgBuffer: string[];
  public _connectedOnce: boolean;

  public _backoffStrategy: any;
  public _badMessagesCount: number;
  public _reconnectAttempt: number;

  public _connectPromise:
    | Promise<{ response: IncomingMessage | undefined } | undefined>
    | undefined;
  public _nextPingTimeout: any;

  constructor(_: ClientOptions) {
    super();

    this._identity = undefined;
    this._wildcardHandler = undefined;
    this._handlers = new Map();
    this._state = CLOSED; // Ideally use a constant or enum
    this._callQueue = []; // Replace with actual Queue class

    this._ws = undefined;
    this._wsAbortController = undefined;
    this._keepAliveAbortController = undefined;
    this._pendingPingResponse = false;
    this._lastPingTime = 0;
    this._closePromise = undefined;
    this._protocolOptions = [];
    this._protocol = undefined;
    this._strictProtocols = [];
    this._strictValidators = undefined;

    this._pendingCalls = new Map();
    this._pendingResponses = new Map();
    this._outboundMsgBuffer = [];
    this._connectedOnce = false;

    this._backoffStrategy = undefined;
    this._badMessagesCount = 0;
    this._reconnectAttempt = 0;

    this._options = {
      endpoint: "ws://localhost",
      callTimeoutMs: 1000 * 60,
      pingIntervalMs: 1000 * 30,
      deferPingsOnActivity: false,
      wsOpts: {},
      headers: {},
      protocols: [],
      reconnect: true,
      maxReconnects: Infinity,
      respondWithDetailedErrors: false,
      callConcurrency: 1,
      maxBadMessages: Infinity,
      strictMode: false,
      strictModeValidators: [],
      backoff: {
        initalDelay: 1000,
        maxDelay: 10 * 1000,
        factor: 2,
        randomisationFactor: 0.25,
      },
    };
  }

  public get identity(): string | undefined {
    return this._identity;
  }

  public set identity(id: string | undefined) {
    this._identity = id;
  }

  public get state() {
    return this._state;
  }

  reconfigure(options: ClientOptions) {
    const newOpts = Object.assign(this._options, options);

    if (!newOpts.identity) {
      throw Error("identity is required");
    }

    if (newOpts.strictMode && !newOpts.protocols.length) {
      throw Error("strictMode requires at least one subprotocol");
    }

    const strictValidators = [...standardValidators];
    if (newOpts.strictModeValidators) {
      strictValidators.push(...newOpts.strictModeValidators);
    }

    this._strictValidators = strictValidators.reduce((svs, v) => {
      svs.set(v.subprotocol, v);
      return svs;
    }, new Map());

    this._strictProtocols = [];

    if (Array.isArray(newOpts.strictMode)) {
      this._strictProtocols = newOpts.strictMode;
    } else if (newOpts.strictMode) {
      this._strictProtocols = newOpts.protocols;
    }

    const missingValidator = this._strictProtocols.find(
      (protocol) => !this._strictValidators.has(protocol)
    );
    if (missingValidator) {
      throw Error(
        `Missing strictMode validator for subprotocol '${missingValidator}'`
      );
    }

    this._callQueue.setConcurrency(newOpts.callConcurrency);
    this._backoffStrategy = new ExponentialStrategy(newOpts.backoff);

    if ("pingIntervalMs" in options) {
      this._keepAlive();
    }
  }

  /**
   * Attempt to connect to the RPCServer
   * @returns {Promise<void>}
   */
  public async connect(): Promise<
    { response: IncomingMessage | undefined } | undefined
  > {
    this._protocolOptions = this._options.protocols ?? [];
    this._protocol = undefined;
    this._identity = this._options.identity;

    let connUrl: string =
      this._options.endpoint +
      "/" +
      encodeURIComponent(this._options.identity!);

    if (this._options.query) {
      const searchParams = new URLSearchParams(this._options.query);
      connUrl += "?" + searchParams.toString();
    }

    this._connectionUrl = connUrl;

    if (this._state === CLOSING) {
      throw Error("Cannot connect while closing");
    }

    if (this._state === OPEN) {
      return;
    }

    if (this._state === CONNECTING) {
      return this._connectPromise;
    }

    try {
      return await this._beginConnect();
    } catch (error) {
      this._state = CLOSED;
      this.emit("close", { code: 1006, reason: "Abnormal Closure" });
      throw error;
    }
  }

  /**
   * Send a message to the RPCServer. While socket is connecting, the message is queued and send when open.
   * @param {Buffer|String} message - String to send via websocket
   */
  sendRaw(message: string) {
    if (([OPEN, CLOSING] as number[]).includes(this._state) && this._ws) {
      this._ws.send(message);
      this.emit("message", { message, outbound: true });
    } else if (this._state === CONNECTING) {
      this._outboundMsgBuffer.push(message);
    } else {
      throw Error("Cannot send message in this state");
    }
  }

  /**
   * Closes the RPCClient.
   * @param {Object} options - Close options
   * @param {number} options.code - The websocket CloseEvent code.
   * @param {string} options.reason - The websocket CloseEvent reason.
   * @param {boolean} options.awaitPending - Wait for in-flight calls & responses to complete before closing.
   * @param {boolean} options.force - Terminate websocket immediately without passing code, reason, or waiting.
   * @see {@link https://developer.mozilla.org/en-US/docs/Web/API/CloseEvent/code CloseEvent codes}
   * @returns Promise<Object> - The CloseEvent (code & reason) for closure. May be different from requested code & reason.
   */
  async close({ code, reason, awaitPending, force }: IClose) {
    if (([OPEN, CLOSING] as number[]).includes(this._state)) {
      // no-op
      return this._closePromise;
    }

    if (this._state === OPEN && this._ws) {
      this._closePromise = (async () => {
        if (force || !awaitPending) {
          // reject pending calls
          this._rejectPendingCalls("Client going away");
        }

        if (force && this._ws) {
          this._ws.terminate();
        } else if (this._ws) {
          // await pending calls & responses
          await this._awaitUntilPendingSettled();
          if (!code || !isValidStatusCode(code)) {
            code = 1000;
          }
          this._ws.close(code, reason);
        }

        let [codeRes, reasonRes] = await once(this._ws!, "close");

        if (reasonRes instanceof Buffer) {
          reasonRes = reasonRes.toString("utf8");
        }

        return { code: codeRes, reason: reasonRes };
      })();

      this._state = CLOSING;
      this._connectedOnce = false;
      this.emit("closing");

      return this._closePromise;
    } else if (this._wsAbortController) {
      const result = this._connectedOnce
        ? { code, reason }
        : { code: 1001, reason: "Connection aborted" };

      this._wsAbortController.abort();
      this._state = CLOSED;
      this._connectedOnce = false;
      this.emit("close", result);
      return result;
    }
  }

  /**
   *
   * @param {string} [method] - The name of the RPC method to handle.
   * @param {Function} handler - A function that can handle incoming calls for this method.
   */
  handle(method: any, handler: any): void {
    if (method instanceof Function && !handler) {
      this._wildcardHandler = method;
    } else {
      this._handlers.set(method, handler);
    }
  }

  /**
   *
   * @param {string} [method] - The name of the handled method.
   */
  removeHandler(method: string) {
    if (method == null) {
      this._wildcardHandler = undefined;
    } else {
      this._handlers.delete(method);
    }
  }

  removeAllHandlers() {
    this._wildcardHandler = undefined;
    this._handlers.clear();
  }

  /**
   * Call a method on a remote RPCClient or RPCServerClient.
   * @param {string} method - The RPC method to call.
   * @param {*} params - A value to be passed as params to the remote handler.
   * @param {Object} options - Call options
   * @param {number} options.callTimeoutMs - Call timeout (in milliseconds)
   * @param {AbortSignal} options.signal - AbortSignal to cancel the call.
   * @param {boolean} options.noReply - If set to true, the call will return immediately.
   * @returns Promise<*> - Response value from the remote handler.
   */
  async call(
    method: string,
    params: string,
    options: ICallOptions
  ): Promise<void> {
    return await this._callQueue.push(
      this._call.bind(this, method, params, options)
    );
  }

  async _call(method: string, params: string, options: ICallOptions) {
    const timeoutMs = options.callTimeoutMs ?? this._options.callTimeoutMs;

    if (([CLOSED, CLOSING] as number[]).includes(this._state)) {
      throw Error(`Cannot make call while socket not open`);
    }

    const msgId = randomUUID();
    const payload = [MSG_CALL, msgId, method, params];

    if (this._strictProtocols.includes(this._protocol)) {
      // perform some strict-mode checks
      const validator = this._strictValidators.get(this._protocol);
      try {
        validator.validate(`urn:${method}.req`, params);
      } catch (error) {
        this.emit("strictValidationFailure", {
          messageId: msgId,
          method,
          params,
          result: null,
          error,
          outbound: true,
          isCall: true,
        });
        throw error;
      }
    }

    const pendingCall: IPendingCall = { msgId, method, params };

    if (!options.noReply) {
      const timeoutAc = new AbortController();

      const cleanup = () => {
        if (pendingCall.timeout) {
          timeoutAc.abort();
        }
        this._pendingCalls.delete(msgId);
      };

      pendingCall.abort = (reason) => {
        const err = Error(reason);
        err.name = "AbortError";
        pendingCall.reject(err);
      };

      if (options.signal) {
        once(options.signal, "abort").then(() => {
          pendingCall.abort!(options.signal.reason);
        });
      }

      pendingCall.promise = new Promise((resolve, reject) => {
        pendingCall.resolve = (value) => {
          cleanup();
          resolve(value);
        };
        pendingCall.reject = (reason: string) => {
          cleanup();
          reject(reason);
        };
      });

      if (timeoutMs && timeoutMs > 0 && timeoutMs < Infinity) {
        const timeoutError = new TimeoutError("Call timeout");
        pendingCall.timeout = setTimeout(timeoutMs, null, {
          signal: timeoutAc.signal,
        })
          .then(() => {
            pendingCall.reject(timeoutError);
          })
          .catch(() => {});
      }

      this._pendingCalls.set(msgId, pendingCall);
    }

    this.emit("call", { outbound: true, payload });
    this.sendRaw(JSON.stringify(payload));

    if (options.noReply) {
      return;
    }

    try {
      const result = await pendingCall.promise;

      this.emit("callResult", {
        outbound: true,
        messageId: msgId,
        method,
        params,
        result,
      });

      return result;
    } catch (err) {
      this.emit("callError", {
        outbound: true,
        messageId: msgId,
        method,
        params,
        error: err,
      });

      throw err;
    }
  }

  /**
   * Start consuming from a WebSocket
   * @param {WebSocket} ws - A WebSocket instance
   * @param {EventBuffer} leadMsgBuffer - A buffer which traps all 'message' events
   */
  _attachWebsocket(ws: WebSocket, leadMsgBuffer: EventBuffer) {
    ws.once("close", (code, reason) =>
      this._handleDisconnect({ code, reason })
    );
    ws.on("error", (err) => this.emit("socketError", err));
    ws.on("ping", () => {
      if (this._options.deferPingsOnActivity) {
        this._deferNextPing();
      }
    });
    ws.on("pong", () => {
      if (this._options.deferPingsOnActivity) {
        this._deferNextPing();
      }
      this._pendingPingResponse = false;
      const rtt = Date.now() - this._lastPingTime;
      this.emit("ping", { rtt });
    });

    this._keepAlive();

    process.nextTick(() => {
      if (leadMsgBuffer) {
        const messages = leadMsgBuffer.condense();
        messages.forEach(([msg]) => this._onMessage(msg));
      }
      ws.on("message", (msg) => this._onMessage(msg));
    });
  }

  _rejectPendingCalls(abortReason: string) {
    const pendingCalls = Array.from(this._pendingCalls.values());
    const pendingResponses = Array.from(this._pendingResponses.values());
    [...pendingCalls, ...pendingResponses].forEach((c) => c.abort(abortReason));
  }

  async _awaitUntilPendingSettled() {
    const pendingCalls = Array.from(this._pendingCalls.values());
    const pendingResponses = Array.from(this._pendingResponses.values());
    return await Promise.allSettled([
      ...pendingResponses.map((c) => c.promise),
      ...pendingCalls.map((c) => c.promise),
    ]);
  }

  _handleDisconnect({ code, reason }: IDisconnect) {
    if (reason instanceof Buffer) {
      reason = reason.toString("utf8");
    }

    // reject any outstanding calls/responses
    this._rejectPendingCalls("Client disconnected");
    this._keepAliveAbortController?.abort();

    this.emit("disconnect", { code, reason });

    if (this._state === CLOSED) {
      // nothing to do here
      return;
    }

    if (this._state !== CLOSING && this._options.reconnect) {
      this._tryReconnect();
    } else {
      this._state = CLOSED;
      this.emit("close", { code, reason });
    }
  }

  _beginConnect() {
    this._connectPromise = (async () => {
      this._wsAbortController = new AbortController();

      const wsOpts: WsOpts = Object.assign(
        {
          // defaults
          noDelay: true,
          signal: this._wsAbortController.signal,
          headers: {
            "user-agent": getPackageIdent(),
          },
        },
        this._options.wsOpts ?? {}
      );

      Object.assign(wsOpts.headers, this._options.headers);

      if (this._options.password != null) {
        const usernameBuffer = Buffer.from(this._identity + ":");
        let passwordBuffer: string | Buffer = this._options.password;
        if (typeof passwordBuffer === "string") {
          passwordBuffer = Buffer.from(passwordBuffer, "utf8");
        }

        const b64 = Buffer.concat([usernameBuffer, passwordBuffer]).toString(
          "base64"
        );
        wsOpts.headers.authorization = "Basic " + b64;
      }

      this._ws = new WebSocket(
        this._connectionUrl!,
        this._protocolOptions,
        wsOpts
      );

      const leadMsgBuffer = new EventBuffer(this._ws, "message");
      let upgradeResponse: IncomingMessage | undefined;

      try {
        await new Promise<void>((resolve, reject) => {
          this._ws!.once("unexpected-response", (request, response) => {
            const error = new UnexpectedHttpResponse(response.statusMessage!);
            error.code = Number(response.statusCode);
            error.request = request;
            error.response = response;
            reject(error);
          });
          this._ws!.once("upgrade", (response) => {
            upgradeResponse = response;
          });
          this._ws!.once("error", (err) => reject(err));
          this._ws!.once("open", () => resolve());
        });

        // record which protocol was selected
        if (this._protocol === undefined) {
          this._protocol = this._ws.protocol;
          this.emit("protocol", this._protocol);
        }

        // limit protocol options in case of future reconnect
        this._protocolOptions = this._protocol ? [this._protocol] : [];

        this._reconnectAttempt = 0;
        this._backoffStrategy.reset();
        this._state = OPEN;
        this._connectedOnce = true;
        this._pendingPingResponse = false;

        this._attachWebsocket(this._ws, leadMsgBuffer);

        // send queued messages
        if (this._outboundMsgBuffer.length > 0) {
          const buff = this._outboundMsgBuffer;
          this._outboundMsgBuffer = [];
          buff.forEach((msg) => this.sendRaw(msg));
        }

        const result = {
          response: upgradeResponse,
        };

        this.emit("open", result);
        return result;
      } catch (error: any) {
        this._ws.terminate();
        if (upgradeResponse) {
          error.upgrade = upgradeResponse;
        }
        throw error;
      }
    })();

    this._state = CONNECTING;
    this.emit("connecting", { protocols: this._protocolOptions });

    return this._connectPromise;
  }

  _deferNextPing() {
    if (!this._nextPingTimeout) {
      return;
    }

    this._nextPingTimeout.refresh();
  }

  async _keepAlive() {}

  async _tryReconnect() {
    this._reconnectAttempt++;
    if (this._reconnectAttempt > Number(this._options.maxReconnects)) {
      // give up
      this.close({ code: 1001, reason: "Giving up" });
    } else {
      try {
        this._state = CONNECTING;
        const delay = this._backoffStrategy.next();
        await setTimeout(delay, null, {
          signal: this._wsAbortController.signal,
        });

        await this._beginConnect()
          .catch(async (error: Error) => {
            const intolerableErrors = [
              "Maximum redirects exceeded",
              "Server sent no subprotocol",
              "Server sent an invalid subprotocol",
              "Server sent a subprotocol but none was requested",
              "Invalid Sec-WebSocket-Accept header",
            ];

            if (intolerableErrors.includes(error.message)) {
              throw error;
            }

            this._tryReconnect();
          })
          .catch((error: Error) => {
            this.close({ code: 1001, reason: error.message });
          });
      } catch (err) {
        // aborted timeout
        return;
      }
    }
  }

  _onMessage(buffer: string | ArrayBuffer | Buffer[]) {
    if (this._options.deferPingsOnActivity) {
      this._deferNextPing();
    }

    const message = buffer.toString();

    if (!message.length) {
      // ignore empty messages
      // for compatibility with some particular charge point vendors (naming no names)
      return;
    }

    this.emit("message", { message, outbound: false });

    let msgId = "-1";
    let messageType: number = -1;

    try {
      let payload;
      try {
        payload = JSON.parse(message);
      } catch (err) {
        throw createRPCError(
          "RpcFrameworkError",
          "Message must be a JSON structure",
          {}
        );
      }

      if (!Array.isArray(payload)) {
        throw createRPCError(
          "RpcFrameworkError",
          "Message must be an array",
          {}
        );
      }

      const [messageTypePart, msgIdPart, ...more] = payload;

      if (typeof messageTypePart !== "number") {
        throw createRPCError(
          "RpcFrameworkError",
          "Message type must be a number",
          {}
        );
      }

      // Extension fallback mechanism
      // (see section 4.4 of OCPP2.0.1J)
      if (
        ![MSG_CALL, MSG_CALLERROR, MSG_CALLRESULT].includes(messageTypePart)
      ) {
        throw createRPCError(
          "MessageTypeNotSupported",
          "Unrecognised message type",
          {}
        );
      }

      messageType = messageTypePart;

      if (typeof msgIdPart !== "string") {
        throw createRPCError(
          "RpcFrameworkError",
          "Message ID must be a string",
          {}
        );
      }

      msgId = msgIdPart;

      switch (messageType) {
        case MSG_CALL:
          const [method, params] = more;
          if (typeof method !== "string") {
            throw new RPCFrameworkError("Method must be a string");
          }
          this.emit("call", { outbound: false, payload });
          this._onCall(msgId, method, params);
          break;
        case MSG_CALLRESULT:
          const [result] = more;
          this.emit("response", { outbound: false, payload });
          this._onCallResult(msgId, result);
          break;
        case MSG_CALLERROR:
          const [errorCode, errorDescription, errorDetails] = more;
          this.emit("response", { outbound: false, payload });
          this._onCallError(msgId, errorCode, errorDescription, errorDetails);
          break;
        default:
          throw new RPCMessageTypeNotSupportedError(
            `Unexpected message type: ${messageType}`
          );
      }

      this._badMessagesCount = 0;
    } catch (error: any) {
      const shouldClose =
        ++this._badMessagesCount > Number(this._options.maxBadMessages);

      let response = null;
      let errorMessage = "";

      if (![MSG_CALLERROR, MSG_CALLRESULT].includes(messageType)) {
        // We shouldn't respond to CALLERROR or CALLRESULT, but we may respond
        // to any CALL (or other unknown message type) with a CALLERROR
        // (see section 4.4 of OCPP2.0.1J - Extension fallback mechanism)
        const details =
          error.details ||
          (this._options.respondWithDetailedErrors
            ? getErrorPlainObject(error)
            : {});

        errorMessage = error.message || error.rpcErrorMessage || "";

        response = [
          MSG_CALLERROR,
          msgId,
          error.rpcErrorCode || "GenericError",
          errorMessage,
          details ?? {},
        ];
      }

      this.emit("badMessage", { buffer, error, response });

      if (shouldClose) {
        this.close({
          code: 1002,
          reason:
            error instanceof RPCGenericError ? errorMessage : "Protocol error",
        });
      } else if (response && this._state === OPEN) {
        this.sendRaw(JSON.stringify(response));
      }
    }
  }

  async _onCall(msgId: string, method: string, params: any) {
    // NOTE: This method must not throw or else it risks sending 2 replies

    try {
      let payload;

      if (this._state !== OPEN) {
        throw Error("Call received while client state not OPEN");
      }

      try {
        if (this._pendingResponses.has(msgId)) {
          throw createRPCError(
            "RpcFrameworkError",
            `Already processing a call with message ID: ${msgId}`,
            {}
          );
        }

        let handler = this._handlers.get(method);
        if (!handler) {
          handler = this._wildcardHandler;
        }

        if (!handler) {
          throw createRPCError(
            "NotImplemented",
            `Unable to handle '${method}' calls`,
            {}
          );
        }

        if (this._strictProtocols.includes(this._protocol)) {
          // perform some strict-mode checks
          const validator = this._strictValidators.get(this._protocol);
          try {
            validator.validate(`urn:${method}.req`, params);
          } catch (error) {
            this.emit("strictValidationFailure", {
              messageId: msgId,
              method,
              params,
              result: null,
              error,
              outbound: false,
              isCall: true,
            });
            throw error;
          }
        }

        const ac = new AbortController();
        const callPromise = new Promise(async (resolve, reject) => {
          function reply(val: unknown) {
            if (val instanceof Error) {
              reject(val);
            } else {
              resolve(val);
            }
          }

          try {
            if (typeof handler === "function") {
              reply(
                await handler({
                  messageId: msgId,
                  method,
                  params,
                  signal: ac.signal,
                  reply,
                })!
              );
            }
          } catch (err) {
            reply(err);
          }
        });

        const pending = { abort: ac.abort.bind(ac), promise: callPromise };
        this._pendingResponses.set(msgId, pending);
        const result = await callPromise;

        this.emit("callResult", {
          outbound: false,
          messageId: msgId,
          method,
          params,
          result,
        });

        if (result === NOREPLY) {
          return; // don't send a reply
        }

        payload = [MSG_CALLRESULT, msgId, result];

        if (this._strictProtocols.includes(this._protocol)) {
          // perform some strict-mode checks
          const validator = this._strictValidators.get(this._protocol);
          try {
            validator.validate(`urn:${method}.conf`, result);
          } catch (error) {
            this.emit("strictValidationFailure", {
              messageId: msgId,
              method,
              params,
              result,
              error,
              outbound: true,
              isCall: false,
            });
            throw createRPCError("InternalError");
          }
        }
      } catch (error: any) {
        // catch here to prevent this error from being considered a 'badMessage'.

        const details =
          error.details ||
          (this._options.respondWithDetailedErrors
            ? getErrorPlainObject(error)
            : {});

        let rpcErrorCode = error.rpcErrorCode || "GenericError";

        if (this.protocol === "ocpp1.6") {
          // Workaround for some mistakes in the spec in OCPP1.6J
          // (clarified in section 5 of OCPP1.6J errata v1.0)
          switch (rpcErrorCode) {
            case "FormatViolation":
              rpcErrorCode = "FormationViolation";
              break;
            case "OccurenceConstraintViolation":
              rpcErrorCode = "OccurrenceConstraintViolation";
              break;
          }
        }

        payload = [
          MSG_CALLERROR,
          msgId,
          rpcErrorCode,
          error.message || error.rpcErrorMessage || "",
          details ?? {},
        ];

        this.emit("callError", {
          outbound: false,
          messageId: msgId,
          method,
          params,
          error,
        });
      } finally {
        this._pendingResponses.delete(msgId);
      }

      this.emit("response", { outbound: true, payload });
      this.sendRaw(JSON.stringify(payload));
    } catch (err) {
      this.close({ code: 1000, reason: "Unable to send call result" });
    }
  }

  _onCallResult(msgId: string, result: any) {
    const pendingCall = this._pendingCalls.get(msgId);
    if (pendingCall) {
      if (this._strictProtocols.includes(this._protocol)) {
        // perform some strict-mode checks
        const validator = this._strictValidators.get(this._protocol);
        try {
          validator.validate(`urn:${pendingCall.method}.conf`, result);
        } catch (error) {
          this.emit("strictValidationFailure", {
            messageId: msgId,
            method: pendingCall.method,
            params: pendingCall.params,
            result,
            error,
            outbound: false,
            isCall: false,
          });
          return pendingCall.reject(error);
        }
      }

      return pendingCall.resolve(result);
    } else {
      throw createRPCError(
        "RpcFrameworkError",
        `Received CALLRESULT for unrecognised message ID: ${msgId}`,
        {
          msgId,
          result,
        }
      );
    }
  }

  _onCallError(
    msgId: string,
    errorCode: number,
    errorDescription: string,
    errorDetails: Record<string, any> | undefined
  ) {
    const pendingCall = this._pendingCalls.get(msgId);

    if (pendingCall) {
      const error = createRPCError(errorCode, errorDescription, errorDetails);
      pendingCall.reject(error);
    } else {
      throw createRPCError(
        "RpcFrameworkError",
        `Received CALLERROR for unrecognised message ID: ${msgId}`,
        {
          msgId,
          errorCode,
          errorDescription,
          errorDetails,
        }
      );
    }
  }
}
