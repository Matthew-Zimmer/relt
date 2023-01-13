import { Type } from "../type";
import { Location } from '../location';

export type Expression =
  | LetExpression
  | TableExpression
  | FunctionExpression
  | EvalExpression
  | DeclareExpression
  | AssignExpression
  | ConditionalExpression
  | OrExpression
  | AndExpression
  | CmpExpression
  | AddExpression
  | MulExpression
  | UnionExpression
  | JoinExpression
  | GroupByExpression
  | WhereExpression
  | WithExpression
  | DropExpression
  | SelectExpression
  | DotExpression
  | ApplicationExpression
  | IdentifierExpression
  | PlaceholderExpression
  | IntegerExpression
  | FloatExpression
  | StringExpression
  | EnvExpression
  | BooleanExpression
  | NullExpression
  | BlockExpression
  | ObjectExpression
  | ArrayExpression
  | SpreadExpression
  | IndexExpression

export interface LetExpression {
  kind: "LetExpression";
  value: Expression;
  loc: Location;
}

export interface TableExpression {
  kind: "TableExpression";
  value: Expression;
  loc: Location;
}

export interface FunctionExpression {
  kind: "FunctionExpression";
  name?: string;
  args: Expression[];
  value: Expression;
  loc: Location;
}

export interface EvalExpression {
  kind: "EvalExpression";
  node: Expression;
  loc: Location;
}

export interface DeclareExpression {
  kind: "DeclareExpression";
  value: Expression;
  type: Type;
  loc: Location;
}

export interface AssignExpression {
  kind: "AssignExpression";
  left: Expression;
  op: "=";
  right: Expression;
  loc: Location;
}

export interface ConditionalExpression {
  kind: "ConditionalExpression";
  left: Expression;
  op: "??" | "!?";
  right: Expression;
  loc: Location;
}

export interface OrExpression {
  kind: "OrExpression";
  left: Expression;
  op: "||";
  right: Expression;
  loc: Location;
}

export interface AndExpression {
  kind: "AndExpression";
  left: Expression;
  op: "&&";
  right: Expression;
  loc: Location;
}

export interface CmpExpression {
  kind: "CmpExpression";
  left: Expression;
  op: "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: Expression;
  loc: Location;
}

export interface AddExpression {
  kind: "AddExpression";
  left: Expression;
  op: "+" | "-";
  right: Expression;
  loc: Location;
}

export interface MulExpression {
  kind: "MulExpression";
  left: Expression;
  op: "*" | "/" | "%";
  right: Expression;
  loc: Location;
}

export interface UnionExpression {
  kind: "UnionExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface JoinExpression {
  kind: "JoinExpression";
  left: Expression;
  right: Expression;
  method: "inner" | "left" | "right";
  on?: Expression;
  loc: Location;
}

export interface GroupByExpression {
  kind: "GroupByExpression";
  value: Expression;
  by: Expression;
  agg: Expression;
  loc: Location;
}

export interface WhereExpression {
  kind: "WhereExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface WithExpression {
  kind: "WithExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface DropExpression {
  kind: "DropExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface SelectExpression {
  kind: "SelectExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface DotExpression {
  kind: "DotExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface ApplicationExpression {
  kind: "ApplicationExpression";
  left: Expression;
  right: Expression;
  loc: Location;
}

export interface IdentifierExpression {
  kind: "IdentifierExpression";
  name: string;
  loc: Location;
}

export interface PlaceholderExpression {
  kind: "PlaceholderExpression";
  name: string;
  typeCondition?: string;
  kindCondition?: string;
  spread?: { method: "lr" | "rl", overrides: { index: number, value: Expression }[] };
  extract?: string;
  loc: Location;
}

export interface IntegerExpression {
  kind: "IntegerExpression";
  value: number;
  loc: Location;
}

export interface FloatExpression {
  kind: "FloatExpression";
  value: string;
  loc: Location;
}

export interface StringExpression {
  kind: "StringExpression";
  value: string;
  loc: Location;
}

export interface EnvExpression {
  kind: "EnvExpression";
  value: string;
  loc: Location;
}

export interface BooleanExpression {
  kind: "BooleanExpression";
  value: boolean;
  loc: Location;
}

export interface NullExpression {
  kind: "NullExpression";
  loc: Location;
}

export interface BlockExpression {
  kind: "BlockExpression";
  expressions: Expression[];
  loc: Location;
}

export interface ObjectExpression {
  kind: "ObjectExpression";
  properties: Expression[];
  loc: Location;
}

export interface ArrayExpression {
  kind: "ArrayExpression";
  values: Expression[];
  loc: Location;
}

export interface SpreadExpression {
  kind: "SpreadExpression";
  value: Expression;
  loc: Location;
}

export interface IndexExpression {
  kind: "IndexExpression";
  left: Expression;
  index: Expression;
  loc: Location;
}
