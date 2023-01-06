import { TypedExpression } from "../expression/typed";
import { ArrayType, BooleanType, FloatType, ForeignKeyType, IntegerType, PrimaryKeyType, StringType, StructType, Type } from "../type";

export type TypedTypeExpression<T extends Type = Type> =
  | TypedObjectTypeExpression
  | TypedIntegerTypeExpression
  | TypedFloatTypeExpression
  | TypedBooleanTypeExpression
  | TypedStringTypeExpression
  | TypedIdentifierTypeExpression<T>
  | TypedJoinTypeExpression
  | TypedDropTypeExpression
  | TypedWithTypeExpression
  | TypedUnionTypeExpression
  | TypedTypeIntroExpression<T>
  | TypedForeignKeyTypeExpression
  | TypedPrimaryKeyTypeExpression
  | TypedArrayTypeExpression
  | TypedGroupByTypeExpression
  | TypedSortTypeExpression
  | TypedWhereTypeExpression
  | TypedDistinctTypeExpression

export type TypedStructLikeTypeExpression =
  | TypedIdentifierTypeExpression<StructType>
  | TypedJoinTypeExpression
  | TypedDropTypeExpression
  | TypedWithTypeExpression
  | TypedUnionTypeExpression
  | TypedTypeIntroExpression<StructType>
  | TypedGroupByTypeExpression
  | TypedSortTypeExpression
  | TypedWhereTypeExpression
  | TypedDistinctTypeExpression

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
  type: StructType;
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
  left: TypedStructLikeTypeExpression;
  right: TypedStructLikeTypeExpression;
  method: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
  type: StructType;
}

export interface TypedDropTypeExpression {
  kind: "TypedDropTypeExpression";
  left: TypedStructLikeTypeExpression;
  properties: string[];
  type: StructType;
}

export interface TypedWithTypeExpression {
  kind: "TypedWithTypeExpression";
  left: TypedStructLikeTypeExpression;
  rules: TypedRuleProperty[];
  type: StructType;
}

export type TypedRuleProperty =
  | TypedRuleValueProperty
  | TypedRuleTypeProperty

export interface TypedRuleValueProperty {
  kind: "TypedRuleValueProperty";
  name: string;
  value: TypedExpression;
}

export interface TypedRuleTypeProperty {
  kind: "TypedRuleTypeProperty";
  name: string;
  value: TypedTypeExpression;
}

export interface TypedUnionTypeExpression {
  kind: "TypedUnionTypeExpression";
  left: TypedStructLikeTypeExpression;
  right: TypedStructLikeTypeExpression;
  type: StructType;
}

export interface TypedGroupByTypeExpression {
  kind: "TypedGroupByTypeExpression";
  left: TypedStructLikeTypeExpression;
  column: string;
  aggregations: TypedAggProperty[];
  type: StructType;
}

export interface TypedAggProperty {
  kind: "TypedAggProperty";
  name: string;
  value: TypedExpression;
}

export interface TypedSortTypeExpression {
  kind: "TypedSortTypeExpression";
  left: TypedStructLikeTypeExpression;
  columns: { name: string, order: 'asc' | 'desc', nulls: 'first' | 'last' }[];
  type: StructType;
}

export interface TypedWhereTypeExpression {
  kind: "TypedWhereTypeExpression";
  left: TypedStructLikeTypeExpression;
  condition: TypedExpression;
  type: StructType;
}

export interface TypedDistinctTypeExpression {
  kind: "TypedDistinctTypeExpression";
  left: TypedStructLikeTypeExpression;
  columns: string[];
  type: StructType;
}

