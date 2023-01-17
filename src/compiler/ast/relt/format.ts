import { TopLevelExpression, TypedTopLevelExpression } from "./topLevel";
import { format as formatExpression } from './source/format';
import { format as formatTypedExpression } from './typed/format';
import { typeName } from "./type/utils";

export function formatUntyped(e: TopLevelExpression): string {
  const imp = (e: TopLevelExpression): string => {
    switch (e.kind) {
      case "TypeIntroductionExpression":
        return `type ${e.name} = ${typeName(e.type)}`;
      case "SugarDefinition":
        return `sugar ${e.name} ${e.phase}\n\ton ${formatExpression(e.pattern)}\n\tthen ${formatExpression(e.replacement)}`;
      case "SugarDirective":
        return `sugar ${e.command} ${e.name}`;
      default:
        return formatExpression(e);
    }
  }
  return imp(e);
}

export function formatUntypedMany(e: TopLevelExpression[]): string {
  return e.map(formatUntyped).join('\n\n');
}

export function formatTyped(e: TypedTopLevelExpression): string {
  const imp = (e: TypedTopLevelExpression): string => {
    switch (e.kind) {
      case "TypeIntroductionExpression":
        return `type ${e.name} = ${typeName(e.type)}`;
      case "TypedSugarDefinition":
        return `sugar ${e.name} ${e.phase}\n\ton ${formatTypedExpression(e.pattern)}\n\tthen ${formatTypedExpression(e.replacement)}`;
      case "SugarDirective":
        return `sugar ${e.command} ${e.name}`;
      default:
        return formatTypedExpression(e);
    }
  }
  return imp(e);
}

export function formatTypedMany(e: TypedTopLevelExpression[]): string {
  return e.map(formatTyped).join('\n\n');
}
