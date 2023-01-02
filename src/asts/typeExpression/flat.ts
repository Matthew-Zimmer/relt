import { Expression } from "../expression/untyped";
import { Id } from "./util";

export type FlatTypeExpression =
  | PrimitiveFlatTypeExpression
  | FlatObjectTypeExpression
  | FlatJoinTypeExpression
  | FlatDropTypeExpression
  | FlatWithTypeExpression
  | FlatUnionTypeExpression
  | FlatGroupByTypeExpression

export type PrimitiveFlatTypeExpression =
  | FlatIntegerTypeExpression
  | FlatArrayTypeExpression
  | FlatFloatTypeExpression
  | FlatBooleanTypeExpression
  | FlatStringTypeExpression
  | FlatIdentifierTypeExpression
  | FlatTypeIntroExpression
  | FlatForeignKeyTypeExpression
  | FlatPrimaryKeyTypeExpression

export interface NamedExpression {
  name: string;
  value: Expression;
}

export interface FlatTypeIntroExpression {
  kind: "FlatTypeIntroExpression";
  id: Id;
  name: string;
  value: Exclude<FlatTypeExpression, FlatTypeIntroExpression>;
}

export interface FlatObjectTypeExpression {
  kind: "FlatObjectTypeExpression";
  id: Id;
  properties: {
    name: string,
    value: PrimitiveFlatTypeExpression,
  }[];
}

export interface FlatIntegerTypeExpression {
  kind: "FlatIntegerTypeExpression";
  id: Id;
}

export interface FlatFloatTypeExpression {
  kind: "FlatFloatTypeExpression";
  id: Id;
}

export interface FlatBooleanTypeExpression {
  kind: "FlatBooleanTypeExpression";
  id: Id;
}

export interface FlatStringTypeExpression {
  kind: "FlatStringTypeExpression";
  id: Id;
}

export interface FlatIdentifierTypeExpression {
  kind: "FlatIdentifierTypeExpression";
  id: Id;
  name: string;
}

export interface FlatForeignKeyTypeExpression {
  kind: "FlatForeignKeyTypeExpression";
  id: Id;
  table: string;
  column: string;
}

export interface FlatPrimaryKeyTypeExpression {
  kind: "FlatPrimaryKeyTypeExpression";
  id: Id;
  of: FlatStringTypeExpression | FlatIntegerTypeExpression;
}

export interface FlatArrayTypeExpression {
  kind: "FlatArrayTypeExpression";
  id: Id;
  of: PrimitiveFlatTypeExpression;
}

export interface FlatJoinTypeExpression {
  kind: "FlatJoinTypeExpression";
  id: Id;
  left: FlatIdentifierTypeExpression;
  right: FlatIdentifierTypeExpression;
  method: "inner" | "outer" | "left" | "right";
  leftColumn?: string;
  rightColumn?: string;
}

export interface FlatDropTypeExpression {
  kind: "FlatDropTypeExpression";
  id: Id;
  left: FlatIdentifierTypeExpression;
  properties: string[];
}

export interface FlatWithTypeExpression {
  kind: "FlatWithTypeExpression";
  id: Id;
  left: FlatIdentifierTypeExpression;
  rules: NamedExpression[];
}

export interface FlatUnionTypeExpression {
  kind: "FlatUnionTypeExpression";
  id: Id;
  left: FlatIdentifierTypeExpression;
  right: FlatIdentifierTypeExpression;
}

export interface FlatGroupByTypeExpression {
  kind: "FlatGroupByTypeExpression";
  id: Id;
  left: FlatIdentifierTypeExpression;
  column: string | [Id, Id];
  aggregations: NamedExpression[];
}
