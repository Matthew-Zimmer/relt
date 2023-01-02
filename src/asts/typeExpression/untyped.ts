import { Expression } from "../expression/untyped";
import { Id } from "./util";

export type TypeExpression =
  | ObjectTypeExpression
  | IntegerTypeExpression
  | FloatTypeExpression
  | BooleanTypeExpression
  | StringTypeExpression
  | IdentifierTypeExpression
  | JoinTypeExpression
  | DropTypeExpression
  | WithTypeExpression
  | UnionTypeExpression
  | TypeIntroExpression
  | ForeignKeyTypeExpression
  | PrimaryKeyTypeExpression
  | ArrayTypeExpression
  | GroupByTypeExpression

export interface TypeIntroExpression {
  kind: "TypeIntroExpression";
  id: Id;
  name: string;
  value: TypeExpression;
}

export interface ObjectTypeExpression {
  kind: "ObjectTypeExpression";
  id: Id;
  properties: { name: string, value: TypeExpression }[];
}

export interface IntegerTypeExpression {
  kind: "IntegerTypeExpression";
  id: Id;
}

export interface FloatTypeExpression {
  kind: "FloatTypeExpression";
  id: Id;
}

export interface BooleanTypeExpression {
  kind: "BooleanTypeExpression";
  id: Id;
}

export interface StringTypeExpression {
  kind: "StringTypeExpression";
  id: Id;
}

export interface IdentifierTypeExpression {
  kind: "IdentifierTypeExpression";
  id: Id;
  name: string;
}

export interface JoinTypeExpression {
  kind: "JoinTypeExpression";
  id: Id;
  left: TypeExpression;
  right: TypeExpression;
  method: "inner" | "outer" | "left" | "right";
  leftColumn?: string;
  rightColumn?: string;
}

export interface DropTypeExpression {
  kind: "DropTypeExpression";
  id: Id;
  left: TypeExpression;
  properties: string[];
}

export interface WithTypeExpression<R extends RuleProperty = RuleProperty> {
  kind: "WithTypeExpression";
  id: Id;
  left: TypeExpression;
  rules: R[];
}

export type RuleProperty =
  | RuleValueProperty
  | RuleTypeProperty

export interface RuleValueProperty {
  kind: "RuleValueProperty";
  name: string;
  value: Expression;
}

export interface RuleTypeProperty {
  kind: "RuleTypeProperty";
  name: string;
  value: TypeExpression;
}

export interface UnionTypeExpression {
  kind: "UnionTypeExpression";
  id: Id;
  left: TypeExpression;
  right: TypeExpression;
}

export interface ForeignKeyTypeExpression {
  kind: "ForeignKeyTypeExpression";
  id: Id;
  table: string;
  column: string;
}

export interface PrimaryKeyTypeExpression {
  kind: "PrimaryKeyTypeExpression";
  id: Id;
  of: IntegerTypeExpression | StringTypeExpression;
}

export interface ArrayTypeExpression {
  kind: "ArrayTypeExpression";
  id: Id;
  of: TypeExpression;
}

export interface GroupByTypeExpression {
  kind: "GroupByTypeExpression";
  id: Id;
  left: TypeExpression;
  column: string | [Id, Id];
  aggregations: AggProperty[];
}

export interface AggProperty {
  kind: "AggProperty";
  name: string;
  value: Expression;
}
