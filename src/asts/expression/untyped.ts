import { Parameter } from "./parameter";

export type Expression =
  | LetExpression
  | IntegerExpression
  | FloatExpression
  | BooleanExpression
  | StringExpression
  | IdentifierExpression
  | ObjectExpression
  | FunctionExpression
  | BlockExpression
  | ApplicationExpression
  | AddExpression
  | DefaultExpression
  | ArrayExpression
  | DotExpression

export interface LetExpression {
  kind: "LetExpression";
  name: string;
  value: Expression;
}

export interface IntegerExpression {
  kind: "IntegerExpression";
  value: number;
}

export interface FloatExpression {
  kind: "FloatExpression";
  value: number;
}

export interface BooleanExpression {
  kind: "BooleanExpression";
  value: boolean;
}

export interface StringExpression {
  kind: "StringExpression";
  value: string;
}

export interface IdentifierExpression {
  kind: "IdentifierExpression";
  name: string;
}

export interface ObjectExpression {
  kind: "ObjectExpression";
  properties: { name: string, value: Expression }[];
}

export interface BlockExpression {
  kind: "BlockExpression";
  values: Expression[];
}

export interface FunctionExpression {
  kind: "FunctionExpression";
  name: string;
  parameters: Parameter[];
  value: BlockExpression;
}

export interface ApplicationExpression {
  kind: "ApplicationExpression";
  func: Expression;
  args: Expression[];
}

export interface AddExpression {
  kind: "AddExpression";
  left: Expression;
  op: "+";
  right: Expression;
}

export interface DefaultExpression {
  kind: "DefaultExpression";
  left: Expression;
  op: "??";
  right: Expression;
}

export interface ArrayExpression {
  kind: "ArrayExpression";
  values: Expression[];
}

export interface DotExpression {
  kind: "DotExpression";
  left: Expression;
  right: Expression;
}
