export type Type =
  | ObjectType
  | IntegerType
  | FloatType
  | BooleanType
  | StringType
  | FunctionType
  | IdentifierType
  | TypeType
  | UnitType

export interface ObjectType {
  kind: "ObjectType";
  properties: { name: string, type: Type }[];
}

export interface IntegerType {
  kind: "IntegerType";
}

export interface FloatType {
  kind: "FloatType";
}

export interface BooleanType {
  kind: "BooleanType";
}

export interface StringType {
  kind: "StringType";
}

export interface IdentifierType {
  kind: "IdentifierType";
  name: string;
}

export interface FunctionType {
  kind: "FunctionType";
  from: Type[];
  to: Type;
}

export interface TypeType {
  kind: "TypeType";
}

export interface UnitType {
  kind: "UnitType";
}

export const objectType = (...properties: { name: string, type: Type }[]): ObjectType => ({ kind: "ObjectType", properties });
export const integerType = (): IntegerType => ({ kind: "IntegerType" });
export const floatType = (): FloatType => ({ kind: "FloatType" });
export const booleanType = (): BooleanType => ({ kind: "BooleanType" });
export const stringType = (): StringType => ({ kind: "StringType" });
export const functionType = (from: Type[], to: Type): FunctionType => ({ kind: "FunctionType", from, to });
export const identifierType = (name: string): IdentifierType => ({ kind: "IdentifierType", name });
export const typeType = (): TypeType => ({ kind: "TypeType" });
export const unitType = (): UnitType => ({ kind: "UnitType" });
