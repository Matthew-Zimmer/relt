import { Expression } from "./source";
import { TypedExpression } from "./typed";
import { Type } from "./type";
import { Location } from './location';

export type TopLevelExpression =
  | TypeIntroductionExpression
  | Expression
  | SugarDefinition
  | SugarDirective

export type TypedTopLevelExpression =
  | TypeIntroductionExpression
  | TypedExpression
  | TypedSugarDefinition
  | SugarDirective

export interface TypeIntroductionExpression {
  kind: "TypeIntroductionExpression";
  name: string;
  type: Type;
  loc: Location;
}

export interface SugarDefinition {
  kind: "SugarDefinition";
  name: string;
  phase: number;
  pattern: Expression;
  replacement: Expression;
  loc: Location;
}

export interface TypedSugarDefinition {
  kind: "TypedSugarDefinition";
  name: string;
  phase: number;
  pattern: TypedExpression;
  replacement: TypedExpression;
}

export interface SugarDirective {
  kind: "SugarDirective";
  command: "enable" | "disable";
  name: string;
  loc: Location;
}
