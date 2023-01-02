import { TypeIntroExpression, TypeExpression, IntegerTypeExpression, PrimaryKeyTypeExpression } from "../asts/typeExpression/untyped";
import { TopLevelExpression } from "../asts/topLevel";
import { visitTypeExpression } from "../vistors/typeExpression";
import { Visitor, True } from "../vistors/utils";

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
