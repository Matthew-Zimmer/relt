import { mkdir, readFile, rm, writeFile } from "fs/promises";
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
import { readDefaultedReltProject, ReltProject } from "../project";
import { existsSync } from "fs";
import { block, line, nl } from "../asts/line";
import { SparkProject } from "../asts/scala";

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

  const sparkProject = deriveSparkProject(reltProject, typedNamedTypeExpressions, ectx, scope, dependencyGraph);

  return writeScalaProject(reltProject, sparkProject);
}

export async function writeScalaProject(reltProject: Required<ReltProject>, sparkProject: SparkProject) {
  const projectOutDir = `${reltProject.outDir}/${reltProject.name}`;
  const packageDir = reltProject.package.split('.').join('/');

  if (existsSync(projectOutDir))
    await rm(projectOutDir, { recursive: true, force: true });

  await Promise.all([
    mkdir(`${projectOutDir}/project`, { recursive: true }),
    mkdir(`${projectOutDir}/src/main/scala/${packageDir}`, { recursive: true }),
  ]);

  await Promise.all([
    writeFile(`${projectOutDir}/project/build.properties`, generateLines([
      line(`sbt.version = 1.8.0`)
    ])),
    writeFile(`${projectOutDir}/build.sbt`, generateLines([
      line(`name := "libname"`),
      nl,
      line(`version := "0.1"`),
      nl,
      line(`libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.0"`),
      nl,
      line(`val sparkVersion = "3.0.1"`),
      nl,
      line(`libraryDependencies ++= Seq(`),
      block(
        line(`"org.apache.spark" %% "spark-core" % sparkVersion,`),
        line(`"org.apache.spark" %% "spark-sql" % sparkVersion,`),
        line(`"org.apache.spark" %% "spark-mllib" % sparkVersion`),
      ),
      line(`)`),
    ])),
    writeFile(`${projectOutDir}/src/main/scala/${packageDir}/${reltProject.name}.scala`, generateLines(generateSparkProject(sparkProject))),
  ]);
}
