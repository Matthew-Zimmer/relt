
export type Type =
  | IntegerType
  | StringType
  | FloatType
  | BooleanType
  | TableType
  | ObjectType
  | ArrayType
  | NullType
  | OptionalType
  | FunctionType
  | AnyType
  | TupleType
  | NeverType
  | IdentifierType
  | UnitType

export interface IntegerType {
  kind: "IntegerType";
}

export interface UnitType {
  kind: "UnitType";
}

export interface StringType {
  kind: "StringType";
}

export interface FloatType {
  kind: "FloatType";
}

export interface BooleanType {
  kind: "BooleanType";
}

export interface TableType {
  kind: "TableType";
  name: string;
  columns: { name: string, type: Type }[];
}

export interface ObjectType {
  kind: "ObjectType";
  properties: { name: string, type: Type }[];
}

export interface ArrayType {
  kind: "ArrayType";
  of: Type;
}

export interface NullType {
  kind: "NullType";
}

export interface OptionalType<T extends Type = Type> {
  kind: "OptionalType";
  of: T;
}

export interface FunctionType {
  kind: "FunctionType";
  from: Type;
  to: Type;
}

export interface AnyType {
  kind: "AnyType";
}

export interface TupleType {
  kind: "TupleType";
  types: Type[];
}

export interface NeverType {
  kind: "NeverType";
}

export interface IdentifierType {
  kind: "IdentifierType";
  name: string;
}
