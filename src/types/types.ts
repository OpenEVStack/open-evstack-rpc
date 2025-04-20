export type QueueTask<T = any> = {
  fn: () => Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: any) => void;
};

export type AuthCallback = (
  accept: (session?: Record<string, any>, protocol?: string) => void,
  reject: (code?: number, message?: string) => void,
  handshake: any,
  signal: AbortSignal
) => void | Promise<void>;
