import { Expression } from "../ast/relt/source";
import { rewrite } from "../ast/relt/source/utils";
import { SugarDefinition, TopLevelExpression } from "../ast/relt/topLevel";


export function preTypeCheckDesugar(e: TopLevelExpression[]): TopLevelExpression[] {
  let enabledSugars: Record<string, SugarDefinition> = {};
  let disabledSugars: Record<string, SugarDefinition> = {};

  const imp = (e: TopLevelExpression) => {
    switch (e.kind) {
      case "SugarDefinition":
        if (e.phase === 0)
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

      case "LetExpression":
      case "TableExpression":
      case "FunctionExpression":
      case "EvalExpression":
      case "DeclareExpression":
      case "AssignExpression":
      case "ConditionalExpression":
      case "OrExpression":
      case "AndExpression":
      case "CmpExpression":
      case "AddExpression":
      case "MulExpression":
      case "UnionExpression":
      case "JoinExpression":
      case "GroupByExpression":
      case "WhereExpression":
      case "WithExpression":
      case "DropExpression":
      case "SelectExpression":
      case "DotExpression":
      case "ApplicationExpression":
      case "IdentifierExpression":
      case "PlaceholderExpression":
      case "IntegerExpression":
      case "FloatExpression":
      case "StringExpression":
      case "EnvExpression":
      case "BooleanExpression":
      case "NullExpression":
      case "BlockExpression":
      case "ObjectExpression":
      case "ArrayExpression":
      case "SpreadExpression":
      case "IndexExpression": {
        let flag = true;
        const rules = Object.values(enabledSugars);
        while (flag) {
          flag = false;
          for (const rule of rules)
            [e, flag] = rewrite(e as Expression, rule.pattern, rule.replacement);
        }
        return e;
      }
    }
  }

  return e.map(imp);
}
