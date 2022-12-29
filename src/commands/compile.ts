import { readFile } from "fs/promises";
import { parser } from "../parser";
import { Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { generateSparkProject } from "../passes/scalaToLines";
import { typeCheckAllTypeExpressions } from "../passes/typeCheck/typeExpression";
import { linearize } from "../passes/linearize";
import { LinearTypeIntroExpression } from "../asts/typeExpression/linear";
import { namedTypeDependencyGraph } from "../graph";
import { TopLevelExpression } from "../asts/topLevel";
import { gatherNamedTypeExpressions } from "../passes/extractNamedTypeExpressions";
import { typeCheckAllExpressions } from "../passes/typeCheck/expression";
import { evaluateAllExpressions } from "../passes/evaluate";
import { readDefaultedReltProject } from "../project";

export async function compile() {
  const reltProject = await readDefaultedReltProject();

  const fileName = `${reltProject.srcDir}/${reltProject.mainFile}`;

  let fileContent = (await readFile(fileName)).toString();

  fileContent = fileContent.split('\n').map(x => x.replace(/#.*$/, '')).join('\n');

  const module = parser.parse(fileContent);
  const topLevelExpressions: TopLevelExpression[] = module.expressions;

  const expressions = topLevelExpressions.filter(x => x.kind !== 'TypeIntroExpression') as Expression[];

  const [typedExpressions, ectx] = typeCheckAllExpressions(expressions);
  const [values, scope] = evaluateAllExpressions(typedExpressions);

  const namedTypeExpressions = gatherNamedTypeExpressions(topLevelExpressions);

  const linearNamedTypeExpressions = namedTypeExpressions.flatMap(linearize) as LinearTypeIntroExpression[];

  const dependencyGraph = namedTypeDependencyGraph(linearNamedTypeExpressions);

  const [typedNamedTypeExpressions, tctx] = typeCheckAllTypeExpressions(linearNamedTypeExpressions);

  const project = deriveSparkProject(reltProject, typedNamedTypeExpressions, ectx, scope, dependencyGraph);

  const lines = generateSparkProject(project);

  const sourceCode = generateLines(lines);

  console.log(sourceCode);
}
