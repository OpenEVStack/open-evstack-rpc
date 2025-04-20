import { EventEmitter } from "stream";

class EventBuffer {
  private _emitter: EventEmitter;
  private _event: string;
  private _collector: (...args: string[]) => void;
  private _buffer: string[][];

  constructor(emitter: EventEmitter, event: string) {
    this._emitter = emitter;
    this._event = event;

    this._buffer = [];
    this._collector = (...args: string[]) => {
      this._buffer.push(args);
    };

    this._emitter.on(event, this._collector);
  }

  condense(): string[][] {
    this._emitter.off(this._event, this._collector);
    return this._buffer;
  }
}

export default EventBuffer;
