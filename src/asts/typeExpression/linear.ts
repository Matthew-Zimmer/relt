import { Expression } from "../expression/untyped";

export type LinearTypeExpression =
  | PrimitiveLinearTypeExpression
  | LinearObjectTypeExpression
  | LinearJoinTypeExpression
  | LinearDropTypeExpression
  | LinearWithTypeExpression
  | LinearUnionTypeExpression
  | LinearGroupByTypeExpression

export type PrimitiveLinearTypeExpression =
  | LinearIntegerTypeExpression
  | LinearArrayTypeExpression
  | LinearFloatTypeExpression
  | LinearBooleanTypeExpression
  | LinearStringTypeExpression
  | LinearIdentifierTypeExpression
  | LinearTypeIntroExpression
  | LinearForeignKeyTypeExpression
  | LinearPrimaryKeyTypeExpression

export interface LinearTypeIntroExpression {
  kind: "LinearTypeIntroExpression";
  name: string;
  value: Exclude<LinearTypeExpression, LinearTypeIntroExpression>;
}

export interface LinearObjectTypeExpression {
  kind: "LinearObjectTypeExpression";
  properties: {
    name: string,
    value: PrimitiveLinearTypeExpression,
  }[];
}

export interface LinearIntegerTypeExpression {
  kind: "LinearIntegerTypeExpression";
}

export interface LinearFloatTypeExpression {
  kind: "LinearFloatTypeExpression";
}

export interface LinearBooleanTypeExpression {
  kind: "LinearBooleanTypeExpression";
}

export interface LinearStringTypeExpression {
  kind: "LinearStringTypeExpression";
}

export interface LinearIdentifierTypeExpression {
  kind: "LinearIdentifierTypeExpression";
  name: string;
}

export interface LinearJoinTypeExpression {
  kind: "LinearJoinTypeExpression";
  left: LinearIdentifierTypeExpression;
  right: LinearIdentifierTypeExpression;
  type: "inner" | "outer" | "left" | "right";
  leftColumn?: string;
  rightColumn?: string;
}

export interface LinearDropTypeExpression {
  kind: "LinearDropTypeExpression";
  left: LinearIdentifierTypeExpression;
  properties: string[];
}

export interface LinearWithTypeExpression {
  kind: "LinearWithTypeExpression";
  left: LinearIdentifierTypeExpression;
  rules: LinearRuleProperty[];
}

export type LinearRuleProperty =
  | LinearRuleValueProperty
  | LinearRuleTypeProperty

export interface LinearRuleValueProperty {
  kind: "LinearRuleValueProperty";
  name: string;
  value: Expression;
}

export interface LinearRuleTypeProperty {
  kind: "LinearRuleTypeProperty";
  name: string;
  value: LinearTypeExpression;
}

export interface LinearUnionTypeExpression {
  kind: "LinearUnionTypeExpression";
  left: LinearIdentifierTypeExpression;
  right: LinearIdentifierTypeExpression;
}

export interface LinearForeignKeyTypeExpression {
  kind: "LinearForeignKeyTypeExpression";
  table: string;
  column: string;
}

export interface LinearPrimaryKeyTypeExpression {
  kind: "LinearPrimaryKeyTypeExpression";
  of: LinearStringTypeExpression | LinearIntegerTypeExpression;
}

export interface LinearArrayTypeExpression {
  kind: "LinearArrayTypeExpression";
  of: PrimitiveLinearTypeExpression;
}

export interface LinearGroupByTypeExpression {
  kind: "LinearGroupByTypeExpression";
  left: LinearIdentifierTypeExpression;
  column: string;
  aggregations: LinearAggProperty[];
}

export interface LinearAggProperty {
  kind: "LinearAggProperty";
  name: string;
  value: Expression;
}