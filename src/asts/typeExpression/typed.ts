import { TypedExpression } from "../expression/typed";
import { ArrayType, BooleanType, ObjectType, FloatType, ForeignKeyType, IdentifierType, IntegerType, PrimaryKeyType, StringType, Type } from "../type";
import { Id } from "./util";

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
  id: Id;
  name: string;
  value: TypedTypeExpression;
  type: T;
}

export interface TypedIntegerTypeExpression {
  kind: "TypedIntegerTypeExpression";
  id: Id;
  type: IntegerType;
}

export interface TypedFloatTypeExpression {
  kind: "TypedFloatTypeExpression";
  id: Id;
  type: FloatType;
}

export interface TypedBooleanTypeExpression {
  kind: "TypedBooleanTypeExpression";
  id: Id;
  type: BooleanType;
}

export interface TypedStringTypeExpression {
  kind: "TypedStringTypeExpression";
  id: Id;
  type: StringType;
}

export interface TypedForeignKeyTypeExpression {
  kind: "TypedForeignKeyTypeExpression";
  id: Id;
  table: string;
  column: string;
  type: ForeignKeyType;
}

export interface TypedPrimaryKeyTypeExpression {
  kind: "TypedPrimaryKeyTypeExpression";
  id: Id;
  of: TypedIntegerTypeExpression | TypedStringTypeExpression;
  type: PrimaryKeyType;
}

export interface TypedObjectTypeExpression {
  kind: "TypedObjectTypeExpression";
  id: Id;
  properties: { name: string, value: TypedTypeExpression }[];
  type: ObjectType;
}

export interface TypedArrayTypeExpression {
  kind: "TypedArrayTypeExpression";
  id: Id;
  of: TypedTypeExpression;
  type: ArrayType;
}

export interface TypedIdentifierTypeExpression<T extends Type = Type> {
  kind: "TypedIdentifierTypeExpression";
  id: Id;
  name: string;
  type: T;
}

export interface TypedJoinTypeExpression {
  kind: "TypedJoinTypeExpression";
  id: Id;
  left: TypedIdentifierTypeExpression<ObjectType>;
  right: TypedIdentifierTypeExpression<ObjectType>;
  method: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
  type: ObjectType;
}

export interface TypedDropTypeExpression {
  kind: "TypedDropTypeExpression";
  id: Id;
  left: TypedIdentifierTypeExpression<ObjectType>;
  properties: string[];
  type: ObjectType;
}

export interface TypedWithTypeExpression {
  kind: "TypedWithTypeExpression";
  id: Id;
  left: TypedIdentifierTypeExpression<ObjectType>;
  rules: NamedTypedExpression[];
  type: ObjectType;
}

export interface TypedUnionTypeExpression {
  kind: "TypedUnionTypeExpression";
  id: Id;
  left: TypedIdentifierTypeExpression<ObjectType>;
  right: TypedIdentifierTypeExpression<ObjectType>;
  type: ObjectType;
}

export interface TypedGroupByTypeExpression {
  kind: "TypedGroupByTypeExpression";
  id: Id;
  left: TypedIdentifierTypeExpression<ObjectType>;
  column: string;
  aggregations: NamedTypedExpression[];
  type: ObjectType;
}
