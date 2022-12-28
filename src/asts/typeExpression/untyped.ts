
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
  type: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
}

export interface DropTypeExpression {
  kind: "DropTypeExpression";
  left: TypeExpression;
  properties: string[];
}

export interface WithTypeExpression {
  kind: "WithTypeExpression";
  left: TypeExpression;
  right: TypeExpression;
}

export interface UnionTypeExpression {
  kind: "UnionTypeExpression";
  left: TypeExpression;
  right: TypeExpression;
}