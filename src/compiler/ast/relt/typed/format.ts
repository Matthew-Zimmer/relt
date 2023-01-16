import { TypedExpression } from ".";
import { typeName } from "../type/utils";

export function format(e: TypedExpression): string {
  let indentation = "";
  const indent = () => {
    indentation += "  "
  }
  const unindent = () => {
    indentation = indentation.slice(0, -2)
  }
  const pad = (x: string) => `${indentation}${x}`;

  const imp = (e: TypedExpression): string => {
    switch (e.kind) {
      case "TypedLetExpression":
        return pad(`let ${imp(e.value)} # ${typeName(e.type)}`);
      case "TypedTableExpression":
        return pad(`table ${imp(e.value)}`);
      case "TypedFunctionExpression":
        return `func${e.name ? ` ${e.name}` : ``}${e.args.map(x => `(${imp(x)})`)} => ${imp(e.value)} # ${typeName(e.type)}`;
      case "TypedEvalExpression":
        return pad(`($$ ${imp(e.node)})`);
      case "TypedDeclareExpression":
        return pad(`${imp(e.value)}: ${typeName(e.type)}`);
      case "TypedAssignExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedConditionalExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedOrExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedAndExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedCmpExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedAddExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedMulExpression":
        return pad(`${imp(e.left)} ${e.op} ${imp(e.right)}`);
      case "TypedUnionExpression":
        return pad(`${imp(e.left)} union ${imp(e.right)}`);
      case "TypedJoinExpression":
        return `${imp(e.left)} ${e.method} join ${imp(e.right)}${e.on ? `on ${imp(e.on)}` : ``}`;
      case "TypedGroupByExpression":
        return pad(`group ${imp(e.value)} by ${imp(e.by)} agg ${imp(e.agg)}`);
      case "TypedWhereExpression":
        return pad(`${imp(e.left)} where ${imp(e.right)}`);
      case "TypedWithExpression":
        return pad(`${imp(e.left)} with ${imp(e.right)}`);
      case "TypedDropExpression":
        return pad(`${imp(e.left)} drop ${imp(e.right)}`);
      case "TypedSelectExpression":
        return pad(`${imp(e.left)} select ${imp(e.right)}`);
      case "TypedDotExpression":
        return pad(`${imp(e.left)}.${imp(e.right)}`);
      case "TypedApplicationExpression":
        return pad(`(${imp(e.left)} ${imp(e.right)})`);
      case "TypedIdentifierExpression":
        return pad(`${e.name}`);
      case "TypedPlaceholderExpression":
        return `$${e.name}${e.kindCondition ? `~${e.kindCondition}` : ``}${e.typeCondition ? `:${e.typeCondition}` : ``}`;
      case "TypedIntegerExpression":
        return pad(`${e.value}`);
      case "TypedFloatExpression":
        return pad(`${e.value}`);
      case "TypedStringExpression":
        return pad(`"${e.value}"`);
      case "TypedEnvExpression":
        return pad(`$"${e.value}"`);
      case "TypedBooleanExpression":
        return pad(`${e.value}`);
      case "TypedNullExpression":
        return pad(`null`);
      case "TypedBlockExpression": {
        indent();
        const expressions = e.expressions.map(imp).join('\n');
        unindent();
        return pad(`do\n${expressions}\n${indentation}end`);
      }
      case "TypedObjectExpression": {
        indent();
        const properties = e.properties.map(x => `${imp(x)},`).join('\n');
        unindent();
        return pad(`{\n${properties}\n${indentation}}`);
      }
      case "TypedArrayExpression":
        return pad(`[${e.values.map(imp).join(', ')}]`);
      case "TypedSpreadExpression":
        return pad(`...${imp(e.value)}`);
      case "TypedIndexExpression":
        return pad(`${imp(e.left)}[${imp(e.index)}]`);
      case "TypedInternalExpression":
        return `internal`;
    }
  }

  return imp(e);
}

export function formatMany(e: TypedExpression[]): string {
  return e.map(format).join('\n');
}
