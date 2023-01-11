import { readFile } from "fs/promises";
import { inspect } from "util";
import { Expression } from "./ast/relt/source";
import { SugarDefinition, SugarDirective } from "./ast/relt/topLevel";
import { UserError } from "./errors";
import { checkSugars } from "./phases/checkSugar";
import { parse } from "./phases/parse";
import { preTypeCheckDesugar } from "./phases/preTypeCheckDesugar";
// import { deriveTables } from "./phases/deriveTableExpressions.";
// import { deriveTableInfo } from "./phases/derviveTableInfo";

// const e: Expression = {
//   kind: "LetExpression",
//   name: "T",
//   value: {
//     kind: "ApplicationExpression",
//     left: {
//       kind: "IdentifierExpression",
//       name: "table"
//     },
//     right: {
//       kind: "ObjectExpression",
//       properties: []
//     }
//   }
// }

// console.log(JSON.stringify(e));

// const e1 = deriveTables(e);

// console.log(JSON.stringify(e1));

// const [e2, info] = deriveTableInfo(e1);

// console.log(JSON.stringify(e2));
// console.log(JSON.stringify(info));


async function main() {
  const fileContent = (await readFile('test.relt')).toString();

  let ast = parse(fileContent);

  const sugars = ast.filter(x => x.kind === "SugarDefinition" || x.kind === "SugarDirective") as (SugarDefinition | SugarDirective)[];
  checkSugars(sugars);

  ast = preTypeCheckDesugar(ast);

  console.log(inspect(ast, false, null, true));
}

main().catch(e => {
  if (e instanceof UserError)
    console.error(e.message)
  else
    console.error(e)
});
