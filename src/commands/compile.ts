import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { parser } from "../parser";
import { Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
// import { deriveSparkProject } from "../passes/typedReltToScala";
import { generateSparkProject } from "../passes/scalaToLines";
import { isTypeIdentifierOf, typeCheckAllTypeExpressions } from "../passes/typeCheck/typeExpression";
import { flatten } from "../passes/flatten";
import { FlatTypeIntroExpression } from "../asts/typeExpression/flat";
import { namedTypeDependencyGraph } from "../graph";
import { TopLevelExpression } from "../asts/topLevel";
import { gatherNamedTypeExpressions } from "../passes/extractNamedTypeExpressions";
import { typeCheckAllExpressions } from "../passes/typeCheck/expression";
import { evaluateAllExpressions } from "../passes/evaluate";
import { readDefaultedReltProject, ReltProject } from "../project";
import { existsSync } from "fs";
import { block, Line, line, nl } from "../asts/line";
import { SparkProject } from "../asts/scala";
import { generateAllSourceCodeFlat, generateAllSourceCodeTyped, generateAllSourceCodeUntyped } from "../debug/debug";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { TypedTypeIntroExpression } from "../asts/typeExpression/typed";
import { ObjectType } from "../asts/type";

export interface CompileArgs {
  "developer-mode": boolean;
}

async function prepareDeveloperDirectories() {
  if (!existsSync('out'))
    await mkdir('out');
  if (existsSync('out/developer'))
    await rm('out/developer', { recursive: true });
  await mkdir('out/developer', { recursive: true });
}

async function writeDeveloperLog<T>(fileName: string, e: T[], f: (x: T[]) => Line[]) {
  const lines = f(e);
  // await writeDeveloperAst(fileName + '.json', lines);
  return writeFile(fileName, generateLines(lines));
}

async function writeDeveloperAst(fileName: string, x: any) {
  return writeFile(fileName, JSON.stringify(x, undefined, 2));
}

export async function compile(args: CompileArgs) {
  if (args["developer-mode"])
    await prepareDeveloperDirectories();

  const reltProject = await readDefaultedReltProject();

  const fileName = `${reltProject.srcDir}/${reltProject.mainFile}`;

  let fileContent = (await readFile(fileName)).toString();

  fileContent = fileContent.split('\n').map(x => x.replace(/#.*$/, '')).join('\n');

  const module = parser.parse(fileContent);
  const topLevelExpressions: TopLevelExpression[] = module.expressions;

  if (args["developer-mode"]) {
    console.log(`Writing developer untyped checkpoint`);
    await writeDeveloperAst(`out/developer/untyped-ast.json`, topLevelExpressions);
    await writeDeveloperLog(`out/developer/untyped.txt`, topLevelExpressions, generateAllSourceCodeUntyped);
  }

  const expressions = topLevelExpressions.filter(x => x.kind !== 'TypeIntroExpression') as Expression[];

  const [typedExpressions, ectx] = typeCheckAllExpressions(expressions);
  const [values, scope] = evaluateAllExpressions(typedExpressions);

  const namedTypeExpressions = gatherNamedTypeExpressions(topLevelExpressions);

  const flatNamedTypeExpressions = namedTypeExpressions.flatMap(flatten) as FlatTypeIntroExpression[];

  if (args["developer-mode"]) {
    console.log(`Writing developer flat checkpoint`);
    await writeDeveloperLog(`out/developer/flat.txt`, [expressions, flatNamedTypeExpressions].flat(), generateAllSourceCodeFlat);
  }

  const dependencyGraph = namedTypeDependencyGraph(flatNamedTypeExpressions);

  let [typedNamedTypeExpressions, tctx] = typeCheckAllTypeExpressions(flatNamedTypeExpressions);
  const typedNamedTypeExpressions2 = typedNamedTypeExpressions.filter((x): x is TypedTypeIntroExpression<ObjectType> => x.type.kind === 'ObjectType');

  if (args["developer-mode"]) {
    console.log(`Writing developer typed checkpoint`);
    await writeDeveloperLog(`out/developer/typed.txt`, [typedExpressions, typedNamedTypeExpressions].flat(), generateAllSourceCodeTyped);
  }

  const sparkProject = deriveSparkProject(reltProject, typedNamedTypeExpressions2, ectx, scope, dependencyGraph);

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
