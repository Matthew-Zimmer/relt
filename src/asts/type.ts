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
  | UnionType
  | PrimaryKeyType
  | ForeignKeyType
  | ArrayType
  | OptionalType

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

export interface UnionType {
  kind: "UnionType";
  types: Type[];
}

export interface UnionType {
  kind: "UnionType";
  types: Type[];
}

export interface PrimaryKeyType {
  kind: "PrimaryKeyType";
  of: IntegerType | StringType;
}

export interface ForeignKeyType {
  kind: "ForeignKeyType";
  table: string;
  column: string;
  of: IntegerType | StringType | PrimaryKeyType | ForeignKeyType;
}

export interface ArrayType {
  kind: "ArrayType";
  of: Type;
}

export interface OptionalType {
  kind: "OptionalType";
  of: Type;
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
export const unionType = (...types: Type[]): UnionType => ({ kind: "UnionType", types });
export const pkType = (of: IntegerType | StringType): PrimaryKeyType => ({ kind: "PrimaryKeyType", of });
export const fkType = (table: string, column: string, of: ForeignKeyType['of']): ForeignKeyType => ({ kind: "ForeignKeyType", table, column, of });
export const arrayType = (of: Type): ArrayType => ({ kind: "ArrayType", of });
export const optionalType = (of: Type): OptionalType => ({ kind: "OptionalType", of });
