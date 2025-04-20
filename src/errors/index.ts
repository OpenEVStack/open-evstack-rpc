class TimeoutError extends Error {}
class UnexpectedHttpResponse extends Error {
  code: number;
  request: any;
  response: any;
  
  constructor(statusMessage: string) {
    super(statusMessage);
    this.name = this.constructor.name;
    this.code = 0; // Default to 0; will be overwritten later
    this.request = null; // Default to null
    this.response = null; // Default to null
    Error.captureStackTrace(this, this.constructor);
  }
}

class RPCError extends Error {
  rpcErrorMessage = "";
  rpcErrorCode = "GenericError";
}
class RPCGenericError extends RPCError {
  override rpcErrorMessage = "";
  override rpcErrorCode = "GenericError";
}
class RPCNotImplementedError extends RPCError {
  override rpcErrorMessage = "Requested method is not known";
  override rpcErrorCode = "NotImplemented";
}
class RPCNotSupportedError extends RPCError {
  override rpcErrorMessage = "Requested method is recognised but not supported";
  override rpcErrorCode = "NotSupported";
}
class RPCInternalError extends RPCError {
  override rpcErrorMessage =
    "An internal error occurred and the receiver was not able to process the requested method successfully";
  override rpcErrorCode = "InternalError";
}
class RPCProtocolError extends RPCError {
  override rpcErrorMessage = "Payload for method is incomplete";
  override rpcErrorCode = "ProtocolError";
}
class RPCSecurityError extends RPCError {
  override rpcErrorMessage =
    "During the processing of method a security issue occurred preventing receiver from completing the method successfully";
  override rpcErrorCode = "SecurityError";
}
class RPCFormatViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload for the method is syntactically incorrect or not conform the PDU structure for the method";
  override rpcErrorCode = "FormatViolation";
}
class RPCFormationViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload for the method is syntactically incorrect or not conform the PDU structure for the method";
  override rpcErrorCode = "FormationViolation";
}
class RPCPropertyConstraintViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload is syntactically correct but at least one field contains an invalid value";
  override rpcErrorCode = "PropertyConstraintViolation";
}
class RPCOccurenceConstraintViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload for the method is syntactically correct but at least one of the fields violates occurence constraints";
  override rpcErrorCode = "OccurenceConstraintViolation";
}
class RPCOccurrenceConstraintViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload for the method is syntactically correct but at least one of the fields violates occurence constraints";
  override rpcErrorCode = "OccurrenceConstraintViolation";
}
class RPCTypeConstraintViolationError extends RPCError {
  override rpcErrorMessage =
    "Payload for the method is syntactically correct but at least one of the fields violates data type constraints";
  override rpcErrorCode = "TypeConstraintViolation";
}
class RPCMessageTypeNotSupportedError extends RPCError {
  override rpcErrorMessage =
    "A message with a Message Type Number received is not supported by this implementation.";
  override rpcErrorCode = "MessageTypeNotSupported";
}
class RPCFrameworkError extends RPCError {
  override rpcErrorMessage =
    "Content of the call is not a valid RPC Request, for example: MessageId could not be read.";
  override rpcErrorCode = "RpcFrameworkError";
}

class WebsocketUpgradeError extends Error {
  public code: number;

  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

export {
  WebsocketUpgradeError,
  TimeoutError,
  UnexpectedHttpResponse,
  RPCError,
  RPCGenericError,
  RPCNotImplementedError,
  RPCNotSupportedError,
  RPCInternalError,
  RPCProtocolError,
  RPCSecurityError,
  RPCFormatViolationError,
  RPCFormationViolationError, // to allow for mistake in ocpp1.6j spec
  RPCPropertyConstraintViolationError,
  RPCOccurrenceConstraintViolationError,
  RPCOccurenceConstraintViolationError, // to allow for mistake in ocpp1.6j spec
  RPCTypeConstraintViolationError,
  RPCMessageTypeNotSupportedError,
  RPCFrameworkError,
};
