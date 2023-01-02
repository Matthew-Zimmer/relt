import { TypedTypeExpression } from "../asts/typeExpression/typed";
import { TypeExpression } from "../asts/typeExpression/untyped";
import { Visitor } from "./utils";

export function visitTypeExpression<T>(e: TypeExpression, visitor: Visitor<TypeExpression, T>): T[] {
  const data: T[] = [];
  function _visit(e: TypeExpression): boolean {
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
        case "ForeignKeyTypeExpression":
        case "PrimaryKeyTypeExpression":
          return true;
        case "ObjectTypeExpression":
          e.properties.forEach(x => _visit(x.value));
          return true;
        case "JoinTypeExpression":
        case "UnionTypeExpression":
          _visit(e.left);
          _visit(e.right);
          return true;
        case "WithTypeExpression":
          _visit(e.left);
          e.rules.map(r => {
            switch (r.kind) {
              case "RuleTypeProperty":
                _visit(r.value);
            }
          });
          return true;
        case "GroupByTypeExpression":
        case "DropTypeExpression":
          _visit(e.left);
          return true;
        case "TypeIntroExpression":
          _visit(e.value);
          return true;
        case "ArrayTypeExpression":
          _visit(e.of);
          return true;
      }
    else {
      return false;
    }
  }
  _visit(e);
  return data;
}

export function visitTypedTypeExpression<T>(e: TypedTypeExpression, visitor: Visitor<TypedTypeExpression, T>): T[] {
  const data: T[] = [];
  function _visit(e: TypedTypeExpression): boolean {
    const val = visitor.process(e);
    if (val !== null)
      data.push(val);
    if (visitor.shouldVisitChildren(e))
      switch (e.kind) {
        case "TypedIntegerTypeExpression":
        case "TypedFloatTypeExpression":
        case "TypedBooleanTypeExpression":
        case "TypedStringTypeExpression":
        case "TypedIdentifierTypeExpression":
        case "TypedForeignKeyTypeExpression":
        case "TypedPrimaryKeyTypeExpression":
          return true;
        case "TypedObjectTypeExpression":
          e.properties.forEach(x => _visit(x.value));
          return true;
        case "TypedJoinTypeExpression":
        case "TypedUnionTypeExpression":
          _visit(e.left);
          _visit(e.right);
          return true;
        case "TypedWithTypeExpression":
          _visit(e.left);
          e.rules.map(r => {
            switch (r.kind) {
              case "TypedRuleTypeProperty":
                _visit(r.value);
            }
          });
          return true;
        case "TypedGroupByTypeExpression":
        case "TypedDropTypeExpression":
          _visit(e.left);
          return true;
        case "TypedTypeIntroExpression":
          _visit(e.value);
          return true;
        case "TypedArrayTypeExpression":
          _visit(e.of);
          return true;
      }
    else {
      return false;
    }
  }
  _visit(e);
  return data;
}
