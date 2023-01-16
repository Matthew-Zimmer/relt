import { existsSync } from "fs";
import { mkdir, readFile, rm, writeFile } from "fs/promises";
import { formatUntypedMany, formatTypedMany } from "./ast/relt/format";
import { SugarDefinition, SugarDirective, TypedTopLevelExpression } from "./ast/relt/topLevel";
import { TypedApplicationExpression, TypedAssignExpression, TypedDeclareExpression, TypedExpression, TypedIdentifierExpression, TypedInternalExpression } from "./ast/relt/typed";
import { SparkProject } from "./ast/scala";
import { reportInternalError, UserError } from "./errors";
import { checkSugars } from "./phases/checkSugar";
import { desugar } from "./phases/desugar";
import { parse } from "./phases/parse";
import { preTypeCheckDesugar } from "./phases/preTypeCheckDesugar";
import { deriveDefaultTableHook, TableHook, toScala } from "./phases/toScala";
import { Context, Scope, shallowTypeCheck, typeCheck } from "./phases/typecheck";
import { ReltProject } from "./project";
import z from 'zod';
import { fromKids, normalize, ofKind, visit, visitMap, visitVoid } from "./ast/relt/typed/utils";
import { FunctionType, TableType, Type } from "./ast/relt/type";
import { toBuilderStringMany } from "./ast/relt/builder";

function internalFunction(name: string, value: (e: TypedExpression) => TypedExpression, type: FunctionType): TypedAssignExpression {
  return { kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name, type: { kind: "AnyType" } }, op: "=", right: { kind: "TypedInternalExpression", type, value }, type: { kind: "AnyType" } };
}

export function toTypedExpression(x: any): TypedExpression {
  switch (typeof x) {
    case "bigint":
    case "symbol":
    case "undefined":
    case "function":
      reportInternalError(`Cannot convert ${typeof x} to typed expression`);
    case "boolean":
      return { kind: "TypedBooleanExpression", value: x, type: { kind: "BooleanType" } };
    case "number":
      if (Number.isInteger(x))
        return { kind: "TypedIntegerExpression", value: x, type: { kind: "IntegerType" } };
      else
        return { kind: "TypedFloatExpression", value: `${x}`, type: { kind: "FloatType" } };
    case "object":
      if (x === null)
        return { kind: "TypedNullExpression", type: { kind: "NullType" } };
      else if (Array.isArray(x))
        return { kind: "TypedArrayExpression", values: x.map(toTypedExpression), type: { kind: "ArrayType", of: { kind: "AnyType" } } };
      else
        return { kind: "TypedObjectExpression", properties: Object.entries(x).map<TypedAssignExpression<TypedIdentifierExpression>>(x => ({ kind: "TypedAssignExpression", left: { kind: "TypedIdentifierExpression", name: x[0], type: { kind: "AnyType" } }, op: "=", right: toTypedExpression(x[1]), type: { kind: "AnyType" } })), type: { kind: "ObjectType", properties: [] } };
    case "string":
      return { kind: "TypedStringExpression", value: x, type: { kind: "StringType" } };
  }
}

export function toJSUnsafe(e: TypedExpression): unknown {
  switch (e.kind) {
    case "TypedIntegerExpression":
    case "TypedStringExpression":
    case "TypedBooleanExpression":
      return e.value;
    case "TypedFloatExpression":
      return Number(e.value);
    case "TypedObjectExpression": {
      const entries: [string, unknown][] = [];

      for (const prop of e.properties) {
        switch (prop.kind) {
          case "TypedAssignExpression":
            if (prop.left.kind === "TypedIdentifierExpression") {
              entries.push([prop.left.name, toJSUnsafe(prop.right)]);
              break;
            }
          default:
            throw new Error(`Cannot add property of kind ${prop.kind} to JS object`);
        }
      }

      return Object.fromEntries(entries);
    }
    case "TypedArrayExpression":
      return e.values.map(toJSUnsafe);
    case "TypedFunctionExpression": {
      return (...args: unknown[]) => {
        if (args.length != e.args.length)
          reportInternalError(`Bad JS Func call got ${args.length} args need ${e.args.length} args`);
        if (!e.args.every(ofKind('TypedDeclareExpression')))
          reportInternalError(`Bad JS Func`);
        if (!e.args.every(x => ofKind('TypedIdentifierExpression')(x.value)))
          reportInternalError(`Bad JS Func`);
        return toJSUnsafe(normalize(e.value, Object.fromEntries(args.map((x, i) => [(e.args[i] as any).value.name, toTypedExpression(x)])))[0]);
      }
    }
    default:
      throw new Error(`Cannot convert ${e.kind} to JS`);
  }
}

export function toJS<T>(schema: z.Schema<T>, e: TypedExpression): T {
  return schema.parse(toJSUnsafe(e));
}

function deleteLocs(t: Type): Type {
  delete (t as any).loc;
  switch (t.kind) {
    case "FunctionType":
      deleteLocs(t.from);
      deleteLocs(t.to);
      break;
    case "ObjectType":
      t.properties.forEach(x => deleteLocs(x.type));
      break;
    case "OptionalType":
      deleteLocs(t.of);
      break;
    case "TupleType":
      t.types.forEach(deleteLocs);
      break;
    case "ArrayType":
      deleteLocs(t.of);
      break;
  }
  return t;
}

async function main() {
  const fileContent = (await readFile('test.relt')).toString();

  const fileContentWithoutComments = fileContent.split('\n').map(x => {
    const idx = x.indexOf("#");
    return idx === -1 ? x : x.slice(0, idx);
  }).join('\n');

  let ast = parse(fileContentWithoutComments);

  await writeFile('test-0.relt', formatUntypedMany(ast));

  const sugars = ast.filter(x => x.kind === "SugarDefinition" || x.kind === "SugarDirective") as (SugarDefinition | SugarDirective)[];
  checkSugars(sugars);

  ast = preTypeCheckDesugar(ast);

  await writeFile('test-1.relt', formatUntypedMany(ast));

  let ctx: Context = {
    '+': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
    '-': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
    '*': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
    '/': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
    '%': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "IntegerType" } } },
    '==': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '!=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '<=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '>=': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '<': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '>': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '||': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
    '&&': { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "FunctionType", from: { kind: "IntegerType" }, to: { kind: "BooleanType" } } },
  };

  let scope: Scope = {
    relt: {
      kind: "TypedObjectExpression",
      properties: [],
      type: { kind: "ObjectType", properties: [] },
    }
  };
  let tctx: Context = {};

  let tast = ast.map<TypedTopLevelExpression>(x => {
    switch (x.kind) {
      case "SugarDirective":
        return x;
      case "TypeIntroductionExpression":
        tctx = { ...tctx, [x.name]: x.type };
        return x;
      case "SugarDefinition": {
        const pattern = shallowTypeCheck(x.pattern);
        const replacement = shallowTypeCheck(x.replacement);
        return { kind: "TypedSugarDefinition", name: x.name, phase: x.phase, pattern, replacement };
      }
      default: {
        const y = typeCheck(x, ctx, scope, tctx);
        ctx = y[1];
        scope = y[2];
        return y[0];
      }
    }
  });

  await writeFile('test-2.relt', formatTypedMany(tast));

  tast = desugar(tast);

  // await writeFile('tast', toBuilderStringMany(tast.filter(x => x.kind !== "SugarDirective" && x.kind !== "TypeIntroductionExpression" && x.kind !== "TypedSugarDefinition") as TypedExpression[]));

  // throw "STOP";

  await writeFile('test-3.relt', formatTypedMany(tast));

  const scala = await toScala(tast, scope);

  const reltProject: ReltProject = {
    mainFile: "test.relt",
    name: "Test",
    outDir: "out",
    package: "",
    srcDir: "src",
    version: "0.0.0",
  };

  const sparkProject: SparkProject = {
    kind: "SparkProject",
    imports: [],
    name: "Test",
    package: "",
    sourceCode: scala[0],
    types: [],
  };

  await writeScalaProject(reltProject, sparkProject);
}

export async function ensureDirectoryExists(path: string) {
  const parts = path.split('/');
  let p = '.';
  for (const part of parts) {
    p += `/${part}`;
    if (!existsSync(p)) {
      return mkdir(path, { recursive: true });
    }
  }
}

export async function emptyDirectory(path: string) {
  await rm(path, { recursive: true });
  await mkdir(path, { recursive: true });
}

export async function writeScalaProject(reltProject: ReltProject, sparkProject: SparkProject) {
  const projectOutDir = `${reltProject.outDir}/${reltProject.name}`;
  const packageDir = reltProject.package.split('.').join('/');

  await Promise.all([
    ensureDirectoryExists(`${projectOutDir}/project`),
    ensureDirectoryExists(`${projectOutDir}/src/main/scala/${packageDir}`),
  ]);

  await Promise.all([
    emptyDirectory(`${projectOutDir}/project`),
    emptyDirectory(`${projectOutDir}/src/main/scala/${packageDir}`),
  ]);

  await Promise.all([
    writeFile(`${projectOutDir}/project/build.properties`, `sbt.version = 1.8.0\n`),
    writeFile(`${projectOutDir}/build.sbt`, `
name := "${sparkProject.name}"

version := "${reltProject.version}"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.0",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
)\n`),
    //${sparkProject.libraries.map(lib => ("${lib.package}" %% "${lib.name}" %% "${lib.version}"))}
    writeFile(`${projectOutDir}/src/main/scala/${packageDir}/${reltProject.name}.scala`, sparkProject.sourceCode),
  ]);
}

main().catch(e => {
  if (e instanceof UserError)
    console.error("Error: " + e.message)
  else
    console.error(e)
});
