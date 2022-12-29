import { Type } from "../type";
import { Parameter } from "./parameter";

export type TypedExpression =
  | TypedLetExpression
  | TypedIntegerExpression
  | TypedFloatExpression
  | TypedBooleanExpression
  | TypedStringExpression
  | TypedIdentifierExpression
  | TypedObjectExpression
  | TypedFunctionExpression
  | TypedBlockExpression
  | TypedApplicationExpression
  | TypedAddExpression

export interface TypedLetExpression {
  kind: "TypedLetExpression";
  name: string;
  value: TypedExpression;
  type: Type;
}

export interface TypedIntegerExpression {
  kind: "TypedIntegerExpression";
  value: number;
  type: Type;
}

export interface TypedFloatExpression {
  kind: "TypedFloatExpression";
  value: number;
  type: Type;
}

export interface TypedBooleanExpression {
  kind: "TypedBooleanExpression";
  value: boolean;
  type: Type;
}

export interface TypedStringExpression {
  kind: "TypedStringExpression";
  value: string;
  type: Type;
}

export interface TypedIdentifierExpression {
  kind: "TypedIdentifierExpression";
  name: string;
  type: Type;
}

export interface TypedObjectExpression {
  kind: "TypedObjectExpression";
  properties: { name: string, value: TypedExpression }[];
  type: Type;
}

export interface TypedBlockExpression {
  kind: "TypedBlockExpression";
  values: TypedExpression[];
  type: Type;
}

export interface TypedFunctionExpression {
  kind: "TypedFunctionExpression";
  name: string;
  parameters: Parameter[];
  value: TypedBlockExpression;
  type: Type;
}

export interface TypedApplicationExpression {
  kind: "TypedApplicationExpression";
  func: TypedExpression;
  args: TypedExpression[];
  type: Type;
}

export interface TypedAddExpression {
  kind: "TypedAddExpression";
  left: TypedExpression;
  op: "+";
  right: TypedExpression;
  type: Type;
}
