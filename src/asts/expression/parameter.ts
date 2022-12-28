import { TypeExpression } from "../typeExpression/untyped";

export type Parameter =
  | UnboundedParameter
  | BoundedTypeParameter

export interface UnboundedParameter {
  kind: "UnboundedParameter";
  name: string;
  type: TypeExpression;
}

export interface BoundedTypeParameter {
  kind: "BoundedTypeParameter";
  type: TypeExpression;
}