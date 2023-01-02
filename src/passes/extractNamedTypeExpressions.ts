import { TypeIntroExpression, TypeExpression, IntegerTypeExpression, PrimaryKeyTypeExpression } from "../asts/typeExpression/untyped";
import { TopLevelExpression } from "../asts/topLevel";
import { visitTypeExpression } from "../vistors/typeExpression";
import { Visitor, True } from "../vistors/utils";
import { Id } from "../asts/typeExpression/util";

export function gatherNamedTypeExpressions(ast: TopLevelExpression[]): Omit<TypeIntroExpression, 'id'>[] {
  const typeExpressions: TypeExpression[] = [];

  for (const e of ast) {
    switch (e.kind) {
      case 'TypeIntroExpression':
        typeExpressions.push(e);
        break;
    }
  }

  const visitor: Visitor<TypeExpression, TypeIntroExpression> = {
    process: x => x.kind !== 'TypeIntroExpression' ? null : x,
    shouldVisitChildren: True,
  };

  return typeExpressions.flatMap(t => visitTypeExpression(t, visitor));
}

export function addIds(typeExpressions: Omit<TypeIntroExpression, 'id'>[]): [TypeIntroExpression[], Id] {
  let idCount = 0;

  function assignId(e: Omit<TypeExpression, 'id'>): TypeExpression {
    const x = e as TypeExpression;
    switch (x.kind) {
      case "IntegerTypeExpression":
      case "FloatTypeExpression":
      case "BooleanTypeExpression":
      case "StringTypeExpression":
      case "ForeignKeyTypeExpression":
        return { ...x, id: idCount++ };
      case "IdentifierTypeExpression":
        return { ...x, id: idCount++ };
      case "PrimaryKeyTypeExpression":
        return { kind: "PrimaryKeyTypeExpression", of: assignId(x.of) as PrimaryKeyTypeExpression['of'], id: idCount++ };
      case "TypeIntroExpression":
        return { kind: "TypeIntroExpression", name: x.name, value: assignId(x.value), id: idCount++ };
      case "ObjectTypeExpression":
        return { kind: "ObjectTypeExpression", properties: x.properties.map(x => ({ name: x.name, value: assignId(x.value) })), id: idCount++ };
      case "JoinTypeExpression":
        return { ...x, left: assignId(x.left), right: assignId(x.right), id: idCount++ };
      case "DropTypeExpression":
        return { ...x, left: assignId(x.left), id: idCount++ };
      case "WithTypeExpression":
        return {
          ...x, left: assignId(x.left), rules: x.rules.map(r => {
            switch (r.kind) {
              case "RuleTypeProperty":
                return { kind: "RuleTypeProperty", name: r.name, value: assignId(r.value) };
              case "RuleValueProperty":
                return r;
            }
          }), id: idCount++
        };
      case "UnionTypeExpression":
        return { ...x, left: assignId(x.left), right: assignId(x.right), id: idCount++ };
      case "ArrayTypeExpression":
        return { ...x, of: assignId(x.of), id: idCount++ };
      case "GroupByTypeExpression":
        return { ...x, left: assignId(x.left), id: idCount++ };
    }
  }

  return [typeExpressions.map(t => assignId(t) as TypeIntroExpression), idCount];
}