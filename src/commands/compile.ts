import { readFile } from "fs/promises";
import { parser } from "../parser";
import { BlockExpression, Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { generateSparkProject } from "../passes/scalaToLines";
import { typeCheckAll } from "../passes/typeCheck";
import { linearize } from "../passes/linearize";
import { LinearTypeIntroExpression } from "../asts/typeExpression/linear";
import { namedTypeDependencyGraph } from "../graph";
import { TopLevelExpression } from "../asts/topLevel";
import { gatherNamedTypeExpressions } from "../passes/extractNamedTypeExpressions";

export async function compile(fileName: string) {
  let fileContent = (await readFile(fileName)).toString();

  fileContent = fileContent.split('\n').map(x => x.replace(/#.*$/, '')).join('\n');

  const module = parser.parse(fileContent);
  const topLevelExpressions: TopLevelExpression[] = module.expressions;

  const expressions = topLevelExpressions.filter(x => x.kind !== 'TypeIntroExpression') as Expression[];

  const ast: BlockExpression = {
    kind: 'BlockExpression',
    values: expressions,
  };

  const namedTypeExpressions = gatherNamedTypeExpressions(topLevelExpressions);
  const linearNamedTypeExpressions = namedTypeExpressions.flatMap(linearize) as LinearTypeIntroExpression[];
  const dependencyGraph = namedTypeDependencyGraph(linearNamedTypeExpressions);
  const typedNamedTypeExpressions = typeCheckAll(linearNamedTypeExpressions);
  const project = deriveSparkProject(typedNamedTypeExpressions, dependencyGraph);
  const lines = generateSparkProject(project);
  const sourceCode = generateLines(lines);

  console.log(sourceCode);
}
