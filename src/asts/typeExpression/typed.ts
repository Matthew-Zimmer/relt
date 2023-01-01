import { TypedExpression } from "../expression/typed";
import { BooleanType, FloatType, ForeignKeyType, IdentifierType, IntegerType, ObjectType, PrimaryKeyType, StringType, Type } from "../type";

type DeepType = Exclude<Type, IdentifierType>;

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

export interface TypedTypeIntroExpression {
  kind: "TypedTypeIntroExpression";
  name: string;
  value: TypedTypeExpression;
  shallowTypeValue: Type;
  deepTypeValue: DeepType;
}

export interface TypedObjectTypeExpression {
  kind: "TypedObjectTypeExpression";
  properties: { name: string, value: TypedTypeExpression }[];
  shallowTypeValue: ObjectType;
  deepTypeValue: ObjectType;
}

export interface TypedIntegerTypeExpression {
  kind: "TypedIntegerTypeExpression";
  shallowTypeValue: IntegerType;
  deepTypeValue: IntegerType;
}

export interface TypedFloatTypeExpression {
  kind: "TypedFloatTypeExpression";
  shallowTypeValue: FloatType;
  deepTypeValue: FloatType;
}

export interface TypedBooleanTypeExpression {
  kind: "TypedBooleanTypeExpression";
  shallowTypeValue: BooleanType;
  deepTypeValue: BooleanType;
}

export interface TypedStringTypeExpression {
  kind: "TypedStringTypeExpression";
  shallowTypeValue: StringType;
  deepTypeValue: StringType;
}

export interface TypedIdentifierTypeExpression {
  kind: "TypedIdentifierTypeExpression";
  name: string;
  shallowTypeValue: Type;
  deepTypeValue: DeepType;
}

export interface TypedJoinTypeExpression {
  kind: "TypedJoinTypeExpression";
  left: TypedTypeExpression;
  right: TypedTypeExpression;
  type: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
  shallowTypeValue: IdentifierType;
  deepTypeValue: ObjectType;
}

export interface TypedDropTypeExpression {
  kind: "TypedDropTypeExpression";
  left: TypedTypeExpression;
  properties: string[];
  shallowTypeValue: IdentifierType;
  deepTypeValue: ObjectType;
}

export interface TypedWithTypeExpression {
  kind: "TypedWithTypeExpression";
  left: TypedTypeExpression;
  rules: TypedRuleProperty[];
  shallowTypeValue: IdentifierType;
  deepTypeValue: ObjectType;
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
  left: TypedTypeExpression;
  right: TypedTypeExpression;
  shallowTypeValue: IdentifierType;
  deepTypeValue: ObjectType;
}

export interface TypedForeignKeyTypeExpression {
  kind: "TypedForeignKeyTypeExpression";
  table: string;
  column: string;
  shallowTypeValue: ForeignKeyType;
  deepTypeValue: ForeignKeyType;
}

export interface TypedPrimaryKeyTypeExpression {
  kind: "TypedPrimaryKeyTypeExpression";
  of: TypedIntegerTypeExpression | TypedStringTypeExpression;
  shallowTypeValue: PrimaryKeyType;
  deepTypeValue: PrimaryKeyType;
}

export interface TypedArrayTypeExpression {
  kind: "TypedArrayTypeExpression";
  of: TypedTypeExpression;
  shallowTypeValue: Type;
  deepTypeValue: DeepType;
}

export interface TypedGroupByTypeExpression {
  kind: "TypedGroupByTypeExpression";
  left: TypedTypeExpression;
  column: string;
  aggregations: TypedAggProperty[];
  shallowTypeValue: Type;
  deepTypeValue: DeepType;
}

export interface TypedAggProperty {
  kind: "TypedAggProperty";
  name: string;
  value: TypedExpression;
}