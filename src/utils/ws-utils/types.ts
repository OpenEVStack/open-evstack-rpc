export interface AbortHandshakeOptions {
  code: number;
  message?: string;
  headers?: Record<string, string>;
}
