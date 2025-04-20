export interface ClientOptions {
  identity?: string;
  endpoint: string;
  password?: string;
  query?: string;
  callTimeoutMs: number;
  pingIntervalMs: number;
  deferPingsOnActivity: false;
  wsOpts: WsOpts | {};
  headers: {};
  protocols: [];
  reconnect: boolean;
  maxReconnects: number;
  respondWithDetailedErrors: false;
  callConcurrency: 1;
  maxBadMessages: number;
  strictMode: boolean;
  strictModeValidators: [];
  backoff: BackOff;
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