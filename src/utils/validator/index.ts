import Ajv, { ValidateFunction } from "ajv";
import addFormats from "ajv-formats";
import { createRPCError } from "../rpcError";
import { errorCodeLUT } from "./error-codes";

export class Validator {
  private _subprotocol: string;
  private _ajv: Ajv;

  constructor(subprotocol: string, ajv: Ajv) {
    this._subprotocol = subprotocol;
    this._ajv = ajv;
  }

  get subprotocol(): string {
    return this._subprotocol;
  }

  validate(schemaId: string, params: unknown): boolean {
    const validator = this._ajv.getSchema(schemaId) as ValidateFunction;

    if (!validator) {
      throw createRPCError(
        "ProtocolError",
        `Schema '${schemaId}' is missing from subprotocol schema '${this._subprotocol}'`
      );
    }

    const valid = validator(params);
    if (!valid && validator.errors?.length) {
      const [first] = validator.errors;
      const rpcErrorCode = errorCodeLUT[first.keyword] ?? "FormatViolation";

      throw createRPCError(
        rpcErrorCode,
        this._ajv.errorsText(validator.errors),
        {
          errors: validator.errors,
          data: params,
        }
      );
    }

    return true;
  }
}

export function createValidator(
  subprotocol: string,
  json: Record<string, any>
): Validator {
  const ajv = new Ajv({ strictSchema: false });
  addFormats(ajv);
  ajv.addSchema(json);

  ajv.removeKeyword("multipleOf");
  ajv.addKeyword({
    keyword: "multipleOf",
    type: "number",
    compile(schema: number) {
      return (data: number) => {
        const result = data / schema;
        const epsilon = 1e-6;
        return Math.abs(Math.round(result) - result) < epsilon;
      };
    },
    errors: false,
    metaSchema: {
      type: "number",
    },
  });

  return new Validator(subprotocol, ajv);
}
