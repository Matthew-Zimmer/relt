import { Expression } from "./source";
import { Type } from "./type";
import { Location } from './location';

export type TopLevelExpression =
  | TypeIntroExpression
  | Expression
  | SugarDefinition
  | SugarDirective

export interface TypeIntroExpression {
  kind: "TypeIntroExpression";
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

export interface SugarDirective {
  kind: "SugarDirective";
  command: "enable" | "disable";
  name: string;
  loc: Location;
}
