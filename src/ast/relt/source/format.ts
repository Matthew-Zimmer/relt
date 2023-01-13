import { Expression } from ".";
import { typeName } from "../type/utils";

export function format(e: Expression): string {
  let indentation = "";
  const indent = () => {
    indentation += "  "
  }
  const unindent = () => {
    indentation = indentation.slice(0, -2)
  }
  const pad = (x: string) => `${indentation}${x}`;

  const imp = (e: Expression): string => {
    switch (e.kind) {
      case "LetExpression":
        return pad(`let ${imp(e.value)}`);
      case "TableExpression":
        return pad(`table ${imp(e.value)}`);
      case "FunctionExpression":
        return `func${e.name ? ` ${e.name}` : ``}${e.args.map(x => `(${imp(x)})`)} => ${imp(e.value)}`;
      case "EvalExpression":
        return pad(`($$ ${imp(e.node)})`);
      case "DeclareExpression":
        return pad(`${imp(e.value)}: ${typeName(e.type)}`);
      case "AssignExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "ConditionalExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "OrExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "AndExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "CmpExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "AddExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "MulExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "UnionExpression":
        return pad(`${imp(e.left)} union ${imp(e.right)}`);
      case "JoinExpression":
        return `${imp(e.left)} ${e.method} join ${imp(e.right)}${e.on ? `on ${imp(e.on)}` : ``}`;
      case "GroupByExpression":
        return pad(`group ${imp(e.value)} by ${imp(e.by)} agg ${imp(e.agg)}`);
      case "WhereExpression":
        return pad(`${imp(e.left)} where ${imp(e.right)}`);
      case "WithExpression":
        return pad(`${imp(e.left)} with ${imp(e.right)}`);
      case "DropExpression":
        return pad(`${imp(e.left)} drop ${imp(e.right)}`);
      case "SelectExpression":
        return pad(`${imp(e.left)} select ${imp(e.right)}`);
      case "DotExpression":
        return pad(`${imp(e.left)}.${imp(e.right)}`);
      case "ApplicationExpression":
        return pad(`(${imp(e.left)} ${imp(e.right)})`);
      case "IdentifierExpression":
        return pad(`${e.name}`);
      case "PlaceholderExpression":
        return `$${e.name}${e.kindCondition ? `~${e.kindCondition}` : ``}${e.typeCondition ? `:${e.typeCondition}` : ``}`;
      case "IntegerExpression":
        return pad(`${e.value}`);
      case "FloatExpression":
        return pad(`${e.value}`);
      case "StringExpression":
        return pad(`"${e.value}"`);
      case "EnvExpression":
        return pad(`$"${e.value}"`);
      case "BooleanExpression":
        return pad(`${e.value}`);
      case "NullExpression":
        return pad(`null`);
      case "BlockExpression": {
        indent();
        const expressions = e.expressions.map(imp).join('\n');
        unindent();
        return pad(`do\n${expressions}\n${indentation}end`);
      }
      case "ObjectExpression": {
        indent();
        const properties = e.properties.map(x => `${imp(x)},`).join('\n');
        unindent();
        return pad(`{\n${properties}\n${indentation}}`);
      }
      case "ArrayExpression":
        return pad(`[${e.values.map(imp).join(', ')}]`);
      case "SpreadExpression":
        return pad(`...${imp(e.value)}`);
      case "IndexExpression":
        return pad(`${imp(e.left)}[${imp(e.index)}]`);
    }
  }

  return imp(e);
}

export function formatMany(e: Expression[]): string {
  return e.map(format).join('\n');
}
