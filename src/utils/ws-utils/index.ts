import http from "http";
import { Socket } from "net";

/**
 * Gracefully abort a WebSocket handshake.
 */
export function abortHandshake(
  socket: Socket,
  code: number,
  message?: string,
  headers: Record<string, string> = {}
): void {
  if (socket.writable) {
    const responseMessage =
      message ?? http.STATUS_CODES[code] ?? "Unknown Error";

    const responseHeaders = {
      Connection: "close",
      "Content-Type": "text/html",
      "Content-Length": Buffer.byteLength(responseMessage).toString(),
      ...headers,
    };

    const headerString = Object.entries(responseHeaders)
      .map(([k, v]) => `${k}: ${v}`)
      .join("\r\n");

    socket.write(
      `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r\n${headerString}\r\n\r\n${responseMessage}`
    );
  }

  socket.removeAllListeners("error");
  socket.destroy();
}

/**
 * Character lookup table for valid WebSocket subprotocol tokens.
 */
const tokenChars: number[] = new Array(128).fill(0);
[
  0x21, // !
  ...Array.from({ length: 10 }, (_, i) => 0x23 + i), // # to ~
  ...Array.from({ length: 26 }, (_, i) => 0x41 + i), // A-Z
  ...Array.from({ length: 26 }, (_, i) => 0x61 + i), // a-z
  0x2d,
  0x2e,
  0x5f, // -, ., _
].forEach((code) => {
  tokenChars[code] = 1;
});

/**
 * Parses a `Sec-WebSocket-Protocol` header value.
 */
export function parseSubprotocols(header: string): Set<string> {
  const protocols = new Set<string>();
  let start = -1;
  let end = -1;
  let i = 0;

  for (; i < header.length; i++) {
    const code = header.charCodeAt(i);

    if (end === -1 && tokenChars[code] === 1) {
      if (start === -1) start = i;
    } else if (
      i !== 0 &&
      (code === 0x20 /* space */ || code === 0x09) /* tab */
    ) {
      if (end === -1 && start !== -1) end = i;
    } else if (code === 0x2c /* comma */) {
      if (start === -1)
        throw new SyntaxError(`Unexpected character at index ${i}`);
      if (end === -1) end = i;

      const protocol = header.slice(start, end);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }

      protocols.add(protocol);
      start = end = -1;
    } else {
      throw new SyntaxError(`Unexpected character at index ${i}`);
    }
  }

  if (start === -1 || end !== -1) {
    throw new SyntaxError("Unexpected end of input");
  }

  const protocol = header.slice(start, i);
  if (protocols.has(protocol)) {
    throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
  }

  protocols.add(protocol);
  return protocols;
}

/**
 * Validates if a status code is allowed in a WebSocket close frame.
 */
export function isValidStatusCode(code: number): boolean {
  return (
    (code >= 1000 &&
      code <= 1014 &&
      code !== 1004 &&
      code !== 1005 &&
      code !== 1006) ||
    (code >= 3000 && code <= 4999)
  );
}
