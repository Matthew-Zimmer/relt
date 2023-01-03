import { Expression } from "./expression/untyped";
import { Type } from "./type";
import { TypeIntroExpression } from "./typeExpression/untyped";

export type TopLevelExpression =
  | TypeIntroExpression
  | Expression
  | LibraryDeclaration

export interface LibraryDeclaration {
  kind: "LibraryDeclaration";
  name: string;
  package: string;
  version: string;
  members: { name: string, type: Type }[];
}
