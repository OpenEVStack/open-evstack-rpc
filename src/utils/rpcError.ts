import errors from "../errors";
import pkg from "../../package.json";

// Define the error lookup type more strictly
const rpcErrorLUT: Record<
  string,
  new (message: string) => Error & { details?: any }
> = {
  GenericError: errors.RPCGenericError,
  NotImplemented: errors.RPCNotImplementedError,
  NotSupported: errors.RPCNotSupportedError,
  InternalError: errors.RPCInternalError,
  ProtocolError: errors.RPCProtocolError,
  SecurityError: errors.RPCSecurityError,
  FormationViolation: errors.RPCFormationViolationError,
  FormatViolation: errors.RPCFormatViolationError,
  PropertyConstraintViolation: errors.RPCPropertyConstraintViolationError,
  OccurenceConstraintViolation: errors.RPCOccurenceConstraintViolationError,
  OccurrenceConstraintViolation: errors.RPCOccurrenceConstraintViolationError,
  TypeConstraintViolation: errors.RPCTypeConstraintViolationError,
  MessageTypeNotSupported: errors.RPCMessageTypeNotSupportedError,
  RpcFrameworkError: errors.RPCFrameworkError,
};

export function getPackageIdent(): string {
  return `${pkg.name}/${pkg.version} (${process.platform})`;
}

export function getErrorPlainObject(e: Error): Record<string, any> {
  try {
    // Serialize including non-enumerable properties
    return JSON.parse(JSON.stringify(e, Object.getOwnPropertyNames(e)));
  } catch (error: any) {
    return {
      stack: error.stack,
      message: error.message,
    };
  }
}

export function createRPCError(
  type?: string | number,
  message?: string,
  details?: Record<string, any>
): Error & { details?: any } {
  const E = rpcErrorLUT[type as string] ?? errors.RPCGenericError;
  const err = new E(message ?? "");
  err.details = details;
  return err;
}
