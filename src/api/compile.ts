import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { parser } from "../parser";
import { Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
import { generateSparkProject } from "../passes/scalaToLines";
import { typeCheckTypeExpressions } from "../passes/typeCheck/typeExpression";
import { TopLevelExpression } from "../asts/topLevel";
import { gatherNamedTypeExpressions } from "../passes/extractNamedTypeExpressions";
import { typeCheckAllExpressions } from "../passes/typeCheck/expression";
import { readDefaultedReltProject, readReltProject, ReltProject } from "../project";
import { existsSync } from "fs";
import { block, line, nl } from "../asts/line";
import { SparkProject } from "../asts/scala";
import { TypedExpression } from "../asts/expression/typed";
import { TypedTypeExpression } from "../asts/typeExpression/typed";
import { Context } from "../passes/typeCheck/utils";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { emptyDirectory } from "../commands/pull";

export async function readSourceCode(reltProject: Required<ReltProject>): Promise<string> {
  const fileName = `${reltProject.srcDir}/${reltProject.mainFile}`;
  const fileContent = (await readFile(fileName)).toString();
  return fileContent.split('\n').map(x => x.replace(/#.*$/, '')).join('\n');
}

export function parseSourceCode(code: string) {
  const module = parser.parse(code);
  return module.expressions as TopLevelExpression[];
}

export function typeCheck(nodes: TopLevelExpression[]): [TypedExpression[], Context, TypedTypeExpression[], Context] {
  const expressions = nodes.filter(x => x.kind !== 'TypeIntroExpression') as Expression[];

  const [typedExpressions, ectx] = typeCheckAllExpressions(expressions);

  const namedTypeExpressions = gatherNamedTypeExpressions(nodes);

  let [typedNamedTypeExpressions, tctx] = typeCheckTypeExpressions(namedTypeExpressions);

  return [typedExpressions, ectx, typedNamedTypeExpressions, tctx];
}

export async function writeScalaProject(reltProject: Required<ReltProject>, sparkProject: SparkProject) {
  const projectOutDir = `${reltProject.outDir}/${reltProject.name}`;
  const packageDir = reltProject.package.split('.').join('/');

  await Promise.all([
    emptyDirectory(`${projectOutDir}/project`),
    emptyDirectory(`${projectOutDir}/src/main/scala/${packageDir}`),
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
