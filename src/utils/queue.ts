import { QueueTask } from "../types/types";

class Queue {
  private _pending = 0;
  private _concurrency = Infinity;
  private _queue: QueueTask<any>[] = [];

  setConcurrency(concurrency: number): void {
    this._concurrency = concurrency;
    this._next();
  }

  push<T>(fn: () => Promise<T>): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      this._queue.push({ fn, resolve, reject });
      this._next();
    });
  }

  private async _next(): Promise<void> {
    if (this._pending >= this._concurrency) return;

    const job = this._queue.shift();
    if (!job) return;

    this._pending++;
    try {
      const result = await job.fn();
      job.resolve(result);
    } catch (error) {
      job.reject(error);
    } finally {
      this._pending--;
      this._next(); // continue processing remaining jobs
    }
  }
}

export default Queue;
