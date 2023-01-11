import { TypedExpression } from "../ast/relt/typed";
import { rewrite } from "../ast/relt/typed/utils";


export function desugar(e: TypedExpression, rules: [string, TypedExpression, TypedExpression][]): TypedExpression {
  let flag = true;
  while (flag) {
    flag = false;
    for (const rule of rules)
      [e, flag] = rewrite(e, rule[1], rule[2]);
  }
  return e;
}
