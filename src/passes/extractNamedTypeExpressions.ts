import { TypeIntroExpression, TypeExpression } from "../asts/typeExpression/untyped";
import { TopLevelExpression } from "../asts/topLevel";
import { visitTypeExpression } from "../vistors/typeExpression";
import { Visitor, True } from "../vistors/utils";

export function gatherNamedTypeExpressions(ast: TopLevelExpression[]): TypeIntroExpression[] {
  const typeExpressions: TypeExpression[] = [];

  for (const e of ast) {
    switch (e.kind) {
      case 'FunctionExpression':
        e.parameters.forEach(p => {
          typeExpressions.push(p.type);
        });
        break;
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
