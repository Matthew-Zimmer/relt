
export type LinearTypeExpression =
  | PrimitiveLinearTypeExpression
  | LinearObjectTypeExpression
  | LinearJoinTypeExpression
  | LinearDropTypeExpression
  | LinearWithTypeExpression
  | LinearUnionTypeExpression

export type PrimitiveLinearTypeExpression =
  | LinearIntegerTypeExpression
  | LinearFloatTypeExpression
  | LinearBooleanTypeExpression
  | LinearStringTypeExpression
  | LinearIdentifierTypeExpression
  | LinearTypeIntroExpression

export interface LinearTypeIntroExpression {
  kind: "LinearTypeIntroExpression";
  name: string;
  value: Exclude<LinearTypeExpression, LinearTypeIntroExpression>;
}

export interface LinearObjectTypeExpression {
  kind: "LinearObjectTypeExpression";
  properties: {
    name: string,
    value: PrimitiveLinearTypeExpression,
  }[];
}

export interface LinearIntegerTypeExpression {
  kind: "LinearIntegerTypeExpression";
}

export interface LinearFloatTypeExpression {
  kind: "LinearFloatTypeExpression";
}

export interface LinearBooleanTypeExpression {
  kind: "LinearBooleanTypeExpression";
}

export interface LinearStringTypeExpression {
  kind: "LinearStringTypeExpression";
}

export interface LinearIdentifierTypeExpression {
  kind: "LinearIdentifierTypeExpression";
  name: string;
}

export interface LinearJoinTypeExpression {
  kind: "LinearJoinTypeExpression";
  left: LinearIdentifierTypeExpression;
  right: LinearIdentifierTypeExpression;
  type: "inner" | "outer" | "left" | "right";
  leftColumn: string;
  rightColumn: string;
}

export interface LinearDropTypeExpression {
  kind: "LinearDropTypeExpression";
  left: LinearIdentifierTypeExpression;
  properties: string[];
}

// need to thinks
export interface LinearWithTypeExpression {
  kind: "LinearWithTypeExpression";
  left: LinearIdentifierTypeExpression;
  right: LinearIdentifierTypeExpression;
}

export interface LinearUnionTypeExpression {
  kind: "LinearUnionTypeExpression";
  left: LinearIdentifierTypeExpression;
  right: LinearIdentifierTypeExpression;
}
