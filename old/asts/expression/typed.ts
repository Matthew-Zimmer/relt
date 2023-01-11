import { ArrayType, BooleanType, FunctionType, OptionalType, StructType, Type } from "../type";
import { Parameter } from "./parameter";

export type TypedExpression<T extends Type = Type> =
  | TypedIntegerExpression
  | TypedFloatExpression
  | TypedBooleanExpression
  | TypedStringExpression
  | TypedObjectExpression
  | TypedFunctionExpression
  | TypedArrayExpression
  | TypedLetExpression<T>
  | TypedIdentifierExpression<T>
  | TypedBlockExpression<T>
  | TypedApplicationExpression<T>
  | TypedAddExpression<T>
  | TypedDefaultExpression<T>
  | TypedDotExpression<T>
  | TypedCmpExpression

export interface TypedLetExpression<T extends Type = Type> {
  kind: "TypedLetExpression";
  name: string;
  value: TypedExpression;
  type: T;
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

export interface TypedIdentifierExpression<T extends Type = Type> {
  kind: "TypedIdentifierExpression";
  name: string;
  type: T;
}

export interface TypedObjectExpression {
  kind: "TypedObjectExpression";
  properties: { name: string, value: TypedExpression }[];
  type: Type;
}

export interface TypedBlockExpression<T extends Type = Type> {
  kind: "TypedBlockExpression";
  values: TypedExpression[];
  type: T;
}

export interface TypedFunctionExpression {
  kind: "TypedFunctionExpression";
  name: string;
  parameters: Parameter[];
  value: TypedBlockExpression;
  type: Type;
}

export interface TypedApplicationExpression<T extends Type = Type> {
  kind: "TypedApplicationExpression";
  func: TypedExpression<FunctionType>;
  args: TypedExpression[];
  type: T;
}

export interface TypedAddExpression<T extends Type = Type> {
  kind: "TypedAddExpression";
  left: TypedExpression;
  op: "+";
  right: TypedExpression;
  type: T;
}

export interface TypedCmpExpression {
  kind: "TypedCmpExpression";
  left: TypedExpression;
  op: "==" | "!=" | "<=" | ">=" | "<" | ">";
  right: TypedExpression;
  type: BooleanType;
}

export interface TypedDefaultExpression<T extends Type = Type> {
  kind: "TypedDefaultExpression";
  left: TypedExpression<OptionalType>;
  op: "??";
  right: TypedExpression;
  type: T;
}

export interface TypedArrayExpression {
  kind: "TypedArrayExpression";
  values: TypedExpression[];
  type: ArrayType;
}

export interface TypedDotExpression<T extends Type = Type> {
  kind: "TypedDotExpression";
  left: TypedExpression<StructType>;
  right: TypedIdentifierExpression;
  type: T;
}

