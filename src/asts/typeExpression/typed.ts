import { TypedExpression } from "../expression/typed";
import { ArrayType, BooleanType, ObjectType, FloatType, ForeignKeyType, IdentifierType, IntegerType, PrimaryKeyType, StringType, Type } from "../type";

export type TypedTypeExpression =
  | TypedObjectTypeExpression
  | TypedIntegerTypeExpression
  | TypedFloatTypeExpression
  | TypedBooleanTypeExpression
  | TypedStringTypeExpression
  | TypedIdentifierTypeExpression
  | TypedJoinTypeExpression
  | TypedDropTypeExpression
  | TypedWithTypeExpression
  | TypedUnionTypeExpression
  | TypedTypeIntroExpression
  | TypedForeignKeyTypeExpression
  | TypedPrimaryKeyTypeExpression
  | TypedArrayTypeExpression
  | TypedGroupByTypeExpression

export interface NamedTypedExpression {
  name: string;
  value: TypedExpression;
}

export interface TypedTypeIntroExpression<T extends Type = Type> {
  kind: "TypedTypeIntroExpression";
  name: string;
  value: TypedTypeExpression;
  type: T;
}

export interface TypedIntegerTypeExpression {
  kind: "TypedIntegerTypeExpression";
  type: IntegerType;
}

export interface TypedFloatTypeExpression {
  kind: "TypedFloatTypeExpression";
  type: FloatType;
}

export interface TypedBooleanTypeExpression {
  kind: "TypedBooleanTypeExpression";
  type: BooleanType;
}

export interface TypedStringTypeExpression {
  kind: "TypedStringTypeExpression";
  type: StringType;
}

export interface TypedForeignKeyTypeExpression {
  kind: "TypedForeignKeyTypeExpression";
  table: string;
  column: string;
  type: ForeignKeyType;
}

export interface TypedPrimaryKeyTypeExpression {
  kind: "TypedPrimaryKeyTypeExpression";
  of: TypedIntegerTypeExpression | TypedStringTypeExpression;
  type: PrimaryKeyType;
}

export interface TypedObjectTypeExpression {
  kind: "TypedObjectTypeExpression";
  properties: { name: string, value: TypedTypeExpression }[];
  type: ObjectType;
}

export interface TypedArrayTypeExpression {
  kind: "TypedArrayTypeExpression";
  of: TypedTypeExpression;
  type: ArrayType;
}

export interface TypedIdentifierTypeExpression<T extends Type = Type> {
  kind: "TypedIdentifierTypeExpression";
  name: string;
  type: T;
}

export interface TypedJoinTypeExpression {
  kind: "TypedJoinTypeExpression";
  left: TypedIdentifierTypeExpression<ObjectType>;
  right: TypedIdentifierTypeExpression<ObjectType>;
  method: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
  type: ObjectType;
}

export interface TypedDropTypeExpression {
  kind: "TypedDropTypeExpression";
  left: TypedIdentifierTypeExpression<ObjectType>;
  properties: string[];
  type: ObjectType;
}

export interface TypedWithTypeExpression {
  kind: "TypedWithTypeExpression";
  left: TypedIdentifierTypeExpression<ObjectType>;
  rules: NamedTypedExpression[];
  type: ObjectType;
}

export interface TypedUnionTypeExpression {
  kind: "TypedUnionTypeExpression";
  left: TypedIdentifierTypeExpression<ObjectType>;
  right: TypedIdentifierTypeExpression<ObjectType>;
  type: ObjectType;
}

export interface TypedGroupByTypeExpression {
  kind: "TypedGroupByTypeExpression";
  left: TypedIdentifierTypeExpression<ObjectType>;
  column: string;
  aggregations: NamedTypedExpression[];
  type: ObjectType;
}
