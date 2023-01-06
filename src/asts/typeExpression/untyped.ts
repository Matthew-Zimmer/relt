import { Expression } from "../expression/untyped";

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
  | SortTypeExpression
  | DistinctTypeExpression
  | WhereTypeExpression

export interface TypeIntroExpression {
  kind: "TypeIntroExpression";
  name: string;
  value: TypeExpression;
}

export interface ObjectTypeExpression {
  kind: "ObjectTypeExpression";
  properties: { name: string, value: TypeExpression }[];
}

export interface IntegerTypeExpression {
  kind: "IntegerTypeExpression";
}

export interface FloatTypeExpression {
  kind: "FloatTypeExpression";
}

export interface BooleanTypeExpression {
  kind: "BooleanTypeExpression";
}

export interface StringTypeExpression {
  kind: "StringTypeExpression";
}

export interface IdentifierTypeExpression {
  kind: "IdentifierTypeExpression";
  name: string;
}

export interface JoinTypeExpression {
  kind: "JoinTypeExpression";
  left: TypeExpression;
  right: TypeExpression;
  method: "inner" | "outer" | "left" | "right";
  leftColumn?: string;
  rightColumn?: string;
}

export interface DropTypeExpression {
  kind: "DropTypeExpression";
  left: TypeExpression;
  properties: string[];
}

export interface WithTypeExpression {
  kind: "WithTypeExpression";
  left: TypeExpression;
  rules: RuleProperty[];
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
  left: TypeExpression;
  right: TypeExpression;
}

export interface ForeignKeyTypeExpression {
  kind: "ForeignKeyTypeExpression";
  table: string;
  column: string;
}

export interface PrimaryKeyTypeExpression {
  kind: "PrimaryKeyTypeExpression";
  of: IntegerTypeExpression | StringTypeExpression;
}

export interface ArrayTypeExpression {
  kind: "ArrayTypeExpression";
  of: TypeExpression;
}

export interface GroupByTypeExpression {
  kind: "GroupByTypeExpression";
  left: TypeExpression;
  column: string;
  aggregations: AggProperty[];
}

export interface AggProperty {
  kind: "AggProperty";
  name: string;
  value: Expression;
}

export interface SortTypeExpression {
  kind: "SortTypeExpression";
  left: TypeExpression;
  columns: { name: string, order: 'asc' | 'desc', nulls: 'first' | 'last' }[];
}

export interface DistinctTypeExpression {
  kind: "DistinctTypeExpression";
  left: TypeExpression;
  columns: string[];
}

export interface WhereTypeExpression {
  kind: "WhereTypeExpression";
  left: WhereTypeExpression;
  condition: Expression;
}
