import { Expression } from "../expression/untyped";

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
  name: string;
  value: Exclude<FlatTypeExpression, FlatTypeIntroExpression>;
}

export interface FlatObjectTypeExpression {
  kind: "FlatObjectTypeExpression";
  properties: {
    name: string,
    value: PrimitiveFlatTypeExpression,
  }[];
}

export interface FlatIntegerTypeExpression {
  kind: "FlatIntegerTypeExpression";
}

export interface FlatFloatTypeExpression {
  kind: "FlatFloatTypeExpression";
}

export interface FlatBooleanTypeExpression {
  kind: "FlatBooleanTypeExpression";
}

export interface FlatStringTypeExpression {
  kind: "FlatStringTypeExpression";
}

export interface FlatIdentifierTypeExpression {
  kind: "FlatIdentifierTypeExpression";
  name: string;
}

export interface FlatForeignKeyTypeExpression {
  kind: "FlatForeignKeyTypeExpression";
  table: string;
  column: string;
}

export interface FlatPrimaryKeyTypeExpression {
  kind: "FlatPrimaryKeyTypeExpression";
  of: FlatStringTypeExpression | FlatIntegerTypeExpression;
}

export interface FlatArrayTypeExpression {
  kind: "FlatArrayTypeExpression";
  of: PrimitiveFlatTypeExpression;
}

export interface FlatJoinTypeExpression {
  kind: "FlatJoinTypeExpression";
  left: FlatIdentifierTypeExpression;
  right: FlatIdentifierTypeExpression;
  method: "inner" | "outer" | "left" | "right";
  leftColumn?: string;
  rightColumn?: string;
}

export interface FlatDropTypeExpression {
  kind: "FlatDropTypeExpression";
  left: FlatIdentifierTypeExpression;
  properties: string[];
}

export interface FlatWithTypeExpression {
  kind: "FlatWithTypeExpression";
  left: FlatIdentifierTypeExpression;
  rules: NamedExpression[];
}

export interface FlatUnionTypeExpression {
  kind: "FlatUnionTypeExpression";
  left: FlatIdentifierTypeExpression;
  right: FlatIdentifierTypeExpression;
}

export interface FlatGroupByTypeExpression {
  kind: "FlatGroupByTypeExpression";
  left: FlatIdentifierTypeExpression;
  column: string;
  aggregations: NamedExpression[];
}
