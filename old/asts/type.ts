export type Type =
  | IntegerType
  | FloatType
  | BooleanType
  | StringType
  | FunctionType
  | UnitType
  | UnionType
  | ArrayType
  | OptionalType
  | StructType

export interface StructType {
  kind: "StructType";
  name: string;
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

export interface FunctionType {
  kind: "FunctionType";
  from: Type[];
  to: Type;
}

export interface UnitType {
  kind: "UnitType";
}

export interface UnionType {
  kind: "UnionType";
  types: Type[];
}

export interface ArrayType<T extends Type = Type> {
  kind: "ArrayType";
  of: T;
}

export interface OptionalType<T extends Type = Type> {
  kind: "OptionalType";
  of: T;
}

export const structType = (name: string, properties: { name: string, type: Type }[]): StructType => ({ kind: "StructType", name, properties });
export const integerType = (): IntegerType => ({ kind: "IntegerType" });
export const floatType = (): FloatType => ({ kind: "FloatType" });
export const booleanType = (): BooleanType => ({ kind: "BooleanType" });
export const stringType = (): StringType => ({ kind: "StringType" });
export const functionType = (from: Type[], to: Type): FunctionType => ({ kind: "FunctionType", from, to });
export const unitType = (): UnitType => ({ kind: "UnitType" });
export const unionType = (...types: Type[]): UnionType => ({ kind: "UnionType", types });
export const arrayType = (of: Type): ArrayType => ({ kind: "ArrayType", of });
export const optionalType = (of: Type): OptionalType => ({ kind: "OptionalType", of });
