import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { parser } from "../parser";
import { Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
import { generateSparkProject } from "../passes/scalaToLines";
import { typeCheckTypeExpressions } from "../passes/typeCheck/typeExpression";
import { LibraryDeclaration, TopLevelExpression } from "../asts/topLevel";
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
import { structType } from "../asts/type";

export async function readSourceCode(reltProject: ReltProject): Promise<string> {
  const fileName = `${reltProject.srcDir}/${reltProject.mainFile}`;
  const fileContent = (await readFile(fileName)).toString();
  return fileContent.split('\n').map(x => x.replace(/#.*$/, '')).join('\n');
}

export function parseSourceCode(code: string) {
  const module = parser.parse(code);
  return module.expressions as TopLevelExpression[];
}

export function typeCheck(nodes: TopLevelExpression[]): [TypedExpression[], Context, TypedTypeExpression[], Context, LibraryDeclaration[]] {
  const expressions = nodes.filter(x => x.kind !== 'TypeIntroExpression' && x.kind !== 'LibraryDeclaration') as Expression[];
  const libraryDeclarations = nodes.filter(x => x.kind === 'LibraryDeclaration') as LibraryDeclaration[];

  const initialExpressionContext = Object.fromEntries(libraryDeclarations.map(x => [x.name, structType(x.name, x.members)]));
  const [typedExpressions, ectx] = typeCheckAllExpressions(expressions, initialExpressionContext);

  const namedTypeExpressions = gatherNamedTypeExpressions(nodes);

  let [typedNamedTypeExpressions, tctx] = typeCheckTypeExpressions(namedTypeExpressions, initialExpressionContext);

  return [typedExpressions, ectx, typedNamedTypeExpressions, tctx, libraryDeclarations];
}

export async function writeScalaProject(reltProject: ReltProject, sparkProject: SparkProject) {
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
      line(`name := "${sparkProject.name}"`),
      nl,
      line(`version := "${reltProject.version}"`),
      nl,
      line(`libraryDependencies ++= Seq(`),
      block(
        line(`"org.scala-lang" % "scala-library" % "2.12.0",`),
        line(`"org.apache.spark" %% "spark-core" % "3.0.1",`),
        line(`"org.apache.spark" %% "spark-sql" % "3.0.1",`),
        line(`"org.apache.spark" %% "spark-mllib" % "3.0.1",`),
        ...sparkProject.libraries.map(lib => (
          line(`"${lib.package}" %% "${lib.name}" %% "${lib.version}",`)
        ))
      ),
      line(`)`),
    ])),
    writeFile(`${projectOutDir}/src/main/scala/${packageDir}/${reltProject.name}.scala`, generateLines(generateSparkProject(sparkProject))),
  ]);
}
