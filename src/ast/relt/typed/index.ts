import { TableType, Type } from '../type';

export type TypedExpression =
  | TypedLetExpression
  | TypedIntegerExpression
  | TypedIdentifierExpression
  | TypedApplicationExpression
  | TypedStringExpression
  | TypedBooleanExpression
  | TypedFloatExpression
  | TypedObjectExpression
  | TypedArrayExpression
  | TypedPlaceholderExpression
  | TypedEvalExpression
  | TypedTableExpression
  | TypedDeclareExpression
  | TypedAssignExpression
  | TypedNullExpression

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

export interface TypedIdentifierExpression {
  kind: "TypedIdentifierExpression";
  name: string;
  type: Type;
}

export interface TypedApplicationExpression {
  kind: "TypedApplicationExpression";
  left: TypedExpression;
  right: TypedExpression;
  type: Type;
}

export interface TypedStringExpression {
  kind: "TypedStringExpression";
  value: string;
  type: Type;
}

export interface TypedBooleanExpression {
  kind: "TypedBooleanExpression";
  value: boolean;
  type: Type;
}

export interface TypedFloatExpression {
  kind: "TypedFloatExpression";
  value: string;
  type: Type;
}

export type ValidObjectPropertyExpression =
  | TypedAssignExpression<TypedIdentifierExpression>

export interface TypedObjectExpression {
  kind: "TypedObjectExpression";
  properties: ValidObjectPropertyExpression[];
  type: Type;
}

export interface TypedArrayExpression {
  kind: "TypedArrayExpression";
  values: TypedExpression[];
  type: Type;
}

export interface TypedPlaceholderExpression {
  kind: "TypedPlaceholderExpression";
  name: string;
  type: Type;
}

export interface TypedEvalExpression {
  kind: "TypedEvalExpression";
  node: TypedExpression;
  type: Type;
}

export type ValidTableColumnExpression =
  | TypedDeclareExpression<TypedIdentifierExpression>

export interface TypedTableExpression {
  kind: "TypedTableExpression";
  name: string;
  columns: ValidTableColumnExpression[];
  type: TableType;
}

export interface TypedDeclareExpression<L extends TypedExpression = TypedExpression, T extends Type = Type> {
  kind: "TypedDeclareExpression";
  left: L;
  type: T;
}

export interface TypedAssignExpression<L extends TypedExpression = TypedExpression, R extends TypedExpression = TypedExpression, T extends Type = Type> {
  kind: "TypedAssignExpression";
  left: L;
  right: R;
  type: T;
}

export interface TypedNullExpression {
  kind: "TypedNullExpression";
  type: Type;
}

export function ofKind<K extends TypedExpression['kind']>(kind: K) {
  return <T extends TypedExpression>(x: T): x is T & { kind: K } => x.kind === kind;
}

export function ofType<K extends Type['kind']>(kind: K) {
  return <T extends TypedExpression>(x: T): x is T & { type: Type & { kind: K } } => x.type.kind === kind;
}

export function ofLeft<K extends TypedExpression['kind']>(kind: K) {
  return <T extends { left: TypedExpression }>(x: T): x is T & { left: TypedExpression & { kind: K } } => x.left.kind === kind;
}

export function ofRight<K extends TypedExpression['kind']>(kind: K) {
  return <T extends { right: TypedExpression }>(x: T): x is T & { right: TypedExpression & { kind: K } } => x.right.kind === kind;
}
