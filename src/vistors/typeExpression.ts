import { TypeExpression } from "../asts/typeExpression/untyped";
import { Visitor } from "./utils";

export function visitTypeExpression<T>(e: TypeExpression, visitor: Visitor<TypeExpression, T>): T[] {
  const data: T[] = [];
  function _visit(e: TypeExpression) {
    const val = visitor.process(e);
    if (val !== null)
      data.push(val);
    if (visitor.shouldVisitChildren(e))
      switch (e.kind) {
        case "IntegerTypeExpression":
        case "FloatTypeExpression":
        case "BooleanTypeExpression":
        case "StringTypeExpression":
        case "IdentifierTypeExpression":
          break;
        case "ObjectTypeExpression":
          e.properties.forEach(x => _visit(x.value));
          break;
        case "JoinTypeExpression":
        case "UnionTypeExpression":
        case "WithTypeExpression":
          _visit(e.left);
          break;
        case "DropTypeExpression":
          _visit(e.left);
          break;
        case "TypeIntroExpression":
          _visit(e.value);
          break;
      }
  }
  _visit(e);
  return data;
}
