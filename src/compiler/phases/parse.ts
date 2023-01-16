import { TopLevelExpression } from "../ast/relt/topLevel";
import { parser } from "../parser";

export function parse(fileContent: string): TopLevelExpression[] {
  return parser.parse(fileContent, { grammarSource: "test.relt" }).expressions;
}
