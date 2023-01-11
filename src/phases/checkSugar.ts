import { SugarDefinition, SugarDirective } from "../ast/relt/topLevel";
import { reportUserError } from "../errors";
import { locPath, Location } from '../ast/relt/location';
import { gather, match, ofKind } from "../ast/relt/source/vistor";

export const sugarKindConditionMap = {
  "let": "LetExpression",
  "table": "TableExpression",
  "func": "FunctionExpression",
  "eval": "EvalExpression",
  "declare": "DeclareExpression",
  "ass": "AssignExpression",
  "cond": "ConditionalExpression",
  "or": "OrExpression",
  "and": "AndExpression",
  "cmp": "CmpExpression",
  "add": "AddExpression",
  "mul": "MulExpression",
  "union": "UnionExpression",
  "join": "JoinExpression",
  "group": "GroupByExpression",
  "where": "WhereExpression",
  "with": "WithExpression",
  "drop": "DropExpression",
  "select": "SelectExpression",
  "dot": "DotExpression",
  "app": "ApplicationExpression",
  "id": "IdentifierExpression",
  "placeholder": "PlaceholderExpression",
  "int": "IntegerExpression",
  "float": "FloatExpression",
  "string": "StringExpression",
  "env": "EnvExpression",
  "bool": "BooleanExpression",
  "null": "NullExpression",
  "block": "BlockExpression",
  "obj": "ObjectExpression",
  "arr": "ArrayExpression",
  "spread": "SpreadExpression",
}

export function checkSugars(sugars: (SugarDefinition | SugarDirective)[]) {
  const names = new Map<string, Location>();
  const checkSugarDef = (sugar: SugarDefinition) => {
    if (sugar.name === "all")
      reportUserError(`Sugar cannot be named "all"\nAt ${locPath(sugar.loc)}`);

    if (names.has(sugar.name))
      reportUserError(`Sugar named "${sugar.name}" already defined\nAt ${locPath(sugar.loc)}\nPreviously at ${locPath(names.get(sugar.name)!)}`);
    names.set(sugar.name, sugar.loc);

    const patternPlaceholders = gather(sugar.pattern, ofKind('PlaceholderExpression'));
    const replacePlaceholders = gather(sugar.replacement, ofKind('PlaceholderExpression'));

    const placeHolderNames = new Map(patternPlaceholders.map(x => [x.name, x.loc]));
    for (const ph of replacePlaceholders)
      if (!placeHolderNames.has(ph.name))
        reportUserError(`In sugar "${sugar.name}", placeholder "${ph.name}" is not defined in the pattern\nAt ${locPath(ph.loc)}`);

    for (const ph of replacePlaceholders) {
      if (ph.kindCondition !== undefined)
        reportUserError(`In sugar "${sugar.name}", placeholder "${ph.name}" contains a kind condition which is not allowed in the substitution part of a sugar\nAt ${locPath(ph.loc)}`);
      if (ph.typeCondition !== undefined)
        reportUserError(`In sugar "${sugar.name}", placeholder "${ph.name}" contains a type condition which is not allowed in the substitution part of a sugar\nAt ${locPath(ph.loc)}`);
    }

    for (const ph of patternPlaceholders)
      if (ph.kindCondition !== undefined && !(ph.kindCondition in sugarKindConditionMap))
        reportUserError(`In sugar "${sugar.name}", placeholder "${ph.name}" contains an unknown kind condition "${ph.kindCondition}"\nAt ${locPath(ph.loc)}`);

    if (sugar.phase === 0) {
      for (const ph of patternPlaceholders)
        if (ph.typeCondition !== undefined)
          reportUserError(`In sugar "${sugar.name}", placeholder "${ph.name}" contains a type condition which is not allowed sugar in phase pre type check\nAt ${locPath(ph.loc)}`);
    }

    if (match(sugar.pattern, sugar.replacement) !== undefined) {
      reportUserError(`In sugar "${sugar.name}", pattern matches replacement which is not allowed\nAt ${locPath(sugar.loc)}`);
    }
  }
  const checkSugarDir = (sugar: SugarDirective) => {
    if (sugar.name === "all") return;
    if (names.has(sugar.name)) return;
    reportUserError(`Bad sugar directive cannot ${sugar.command} "${sugar.name}" since a sugar named "${sugar.name}" was not defined`);
  }
  sugars.forEach(x => {
    switch (x.kind) {
      case "SugarDefinition": return checkSugarDef(x);
      case "SugarDirective": return checkSugarDir(x);
    }
  });
}
