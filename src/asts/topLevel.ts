import { Expression } from "./expression/untyped";
import { TypeIntroExpression } from "./typeExpression/untyped";

export type TopLevelExpression =
  | TypeIntroExpression
  | Expression
