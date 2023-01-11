import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { parser } from "../parser";
import { Expression } from "../asts/expression/untyped";
import { generateLines } from "../passes/lineToString";
import { generateSparkProject } from "../passes/scalaToLines";
import { typeCheckTypeExpressions } from "../passes/typeCheck/typeExpression";
import { TopLevelExpression } from "../asts/topLevel";
import { gatherNamedTypeExpressions } from "../passes/extractNamedTypeExpressions";
import { typeCheckAllExpressions } from "../passes/typeCheck/expression";
import { evaluateAllExpressions } from "../passes/evaluate";
import { readDefaultedReltProject, ReltProject } from "../project";
import { existsSync } from "fs";
import { block, Line, line, nl } from "../asts/line";
import { SparkProject } from "../asts/scala";
import { generateAllSourceCodeTyped, generateAllSourceCodeUntyped } from "../debug/debug";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { parseSourceCode, readSourceCode, typeCheck, writeScalaProject } from "../api/compile";

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
  const fileContent = await readSourceCode(reltProject);
  const topLevelExpressions = parseSourceCode(fileContent);

  if (args["developer-mode"]) {
    console.log(`Writing developer untyped checkpoint`);
    await writeDeveloperAst(`out/developer/untyped-ast.json`, topLevelExpressions);
    await writeDeveloperLog(`out/developer/untyped.txt`, topLevelExpressions, generateAllSourceCodeUntyped);
  }

  const [typedExpressions, ectx, typedTypeExpressions, tctx, libs] = typeCheck(topLevelExpressions);

  if (args["developer-mode"]) {
    console.log(`Writing developer typed checkpoint`);
    await writeDeveloperLog(`out/developer/typed.txt`, [typedExpressions, typedTypeExpressions].flat(), generateAllSourceCodeTyped);
  }

  const [values, scope] = evaluateAllExpressions(typedExpressions);

  const sparkProject = deriveSparkProject(reltProject, typedTypeExpressions, ectx, scope, libs);

  return writeScalaProject(reltProject, sparkProject);
}
