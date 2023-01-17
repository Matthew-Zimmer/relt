import { rewrite } from "../ast/relt/typed/utils";
import { SugarDefinition, SugarDirective, TopLevelExpression, TypedSugarDefinition, TypedTopLevelExpression } from "../ast/relt/topLevel";
import { TypedExpression } from "../ast/relt/typed";


export function desugar(e: TypedTopLevelExpression[]): TypedTopLevelExpression[] {
  let enabledSugars: Record<string, TypedSugarDefinition> = {};
  let disabledSugars: Record<string, TypedSugarDefinition> = {};

  const imp = (e: TypedTopLevelExpression) => {
    switch (e.kind) {
      case "TypedSugarDefinition":
        if (e.phase === 1)
          enabledSugars[e.name] = e;
        return e;
      case "SugarDirective":
        switch (e.command) {
          case "disable":
            if (e.name === "all") {
              disabledSugars = { ...disabledSugars, ...enabledSugars };
              enabledSugars = {};
            }
            else if (e.name in enabledSugars) {
              disabledSugars[e.name] = enabledSugars[e.name];
              delete enabledSugars[e.name];
            }
            break;
          case "enable":
            if (e.name === "all") {
              enabledSugars = { ...disabledSugars, ...enabledSugars };
              disabledSugars = {};
            }
            else if (e.name in disabledSugars) {
              enabledSugars[e.name] = disabledSugars[e.name];
              delete disabledSugars[e.name];
            }
            break;
        }
        return e;
      case "TypeIntroductionExpression":
        return e;

      case "TypedLetExpression":
      case "TypedTableExpression":
      case "TypedFunctionExpression":
      case "TypedEvalExpression":
      case "TypedDeclareExpression":
      case "TypedAssignExpression":
      case "TypedConditionalExpression":
      case "TypedOrExpression":
      case "TypedAndExpression":
      case "TypedCmpExpression":
      case "TypedAddExpression":
      case "TypedMulExpression":
      case "TypedUnionExpression":
      case "TypedJoinExpression":
      case "TypedGroupByExpression":
      case "TypedWhereExpression":
      case "TypedWithExpression":
      case "TypedDropExpression":
      case "TypedSelectExpression":
      case "TypedDotExpression":
      case "TypedApplicationExpression":
      case "TypedIdentifierExpression":
      case "TypedPlaceholderExpression":
      case "TypedIntegerExpression":
      case "TypedFloatExpression":
      case "TypedStringExpression":
      case "TypedEnvExpression":
      case "TypedBooleanExpression":
      case "TypedNullExpression":
      case "TypedBlockExpression":
      case "TypedObjectExpression":
      case "TypedArrayExpression":
      case "TypedSpreadExpression":
      case "TypedIndexExpression":
      case "TypedInternalExpression": {
        let flag = true;
        const rules = Object.values(enabledSugars);
        while (flag) {
          flag = false;
          for (const rule of rules)
            [e, flag] = rewrite(e, rule.pattern, rule.replacement);
        }
        return e;
      }
    }
  }

  return e.map(imp);
}
