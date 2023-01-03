import { spawnSync } from "child_process";
import { writeFile } from "fs/promises";
import { readSourceCode, parseSourceCode, typeCheck } from "../api/compile";
import { block, Line, line, nl } from "../asts/line";
import { isSparkSourceDataSet, SparkProject, SparkSourceDataSet } from "../asts/scala";
import { StructType, Type } from "../asts/type";
import { DependencyGraph, namedTypeDependencyGraph } from "../graph";
import { evaluateAllExpressions, typeName } from "../passes/evaluate";
import { generateLines } from "../passes/lineToString";
import { propertyLookup } from "../passes/typeCheck/typeExpression";
import { Context } from "../passes/typeCheck/utils";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { readDefaultedReltProject, ReltProject } from "../project";
import { visitTypedTypeExpression, visitTypedTypeExpressions } from "../vistors/typeExpression";
import { True } from "../vistors/utils";
import { ensureDirectoryExists, emptyDirectory } from './pull';

export interface GenerateArgs {
  resource: string;
  "show-all-types": boolean;
  "skip-compile": boolean;
  "skip-open": boolean;
}

export async function generate(args: GenerateArgs) {
  let { resource } = args;

  const reltProject = await readDefaultedReltProject();
  const fileContent = await readSourceCode(reltProject);
  const topLevelExpressions = parseSourceCode(fileContent);
  const [typedExpressions, ectx, typedTypeExpressions, tctx, libs] = typeCheck(topLevelExpressions);
  const dg = namedTypeDependencyGraph(typedTypeExpressions);
  const [values, scope] = evaluateAllExpressions(typedExpressions);
  const sparkProject = deriveSparkProject(reltProject, typedTypeExpressions, ectx, scope, libs);

  const allStructTypeCtx = Object.fromEntries(visitTypedTypeExpressions(typedTypeExpressions, {
    process: e => e.type.kind === 'StructType' ? e.type : null,
    shouldVisitChildren: True,
  }).map(x => [x.name, x]));

  switch (resource) {
    case "docs":
      await generateDocs(reltProject, dg, allStructTypeCtx, sparkProject, {
        hideImplicits: !args["show-all-types"],
        skipCompile: args["skip-compile"],
        skipOpen: args["skip-open"],
      });
      break;
  }
}

function convertTypeToD2SqlType(t: Type, ctx: Context, root: boolean): string {
  const imp = (t: Type) => convertTypeToD2SqlType(t, ctx, false);
  switch (t.kind) {
    case "PrimaryKeyType":
      return root ? `${imp(t.of)} {constraint: primary_key}` : imp(t.of);
    case "ForeignKeyType":
      return `${imp(propertyLookup(ctx[t.table] as StructType, t.column)!)} {constraint: foreign_key}`
    case "StructType":
      return t.name;
    case "ArrayType":
      return `${imp(t.of)}\\[\\]`;
    case "BooleanType":
      return "bool";
    case "FloatType":
      return "float";
    case "FunctionType":
      return `(${t.from.map(imp).join(', ')}) => ${imp(t.to)}`;
    case "IntegerType":
      return `int`;
    case "OptionalType":
      return `${imp(t.of)}?`;
    case "StringType":
      return `string`;
    case "UnionType":
      return t.types.map(imp).join(' \\| ');
    case "UnitType":
      return 'unit';
  }
}

// TODO add containers
// if readable type add read info
// if writable type add write info
async function generateDocs(
  reltProject: ReltProject,
  dg: DependencyGraph,
  ctx: Context,
  sparkProject: SparkProject,
  options: {
    hideImplicits: boolean,
    skipCompile: boolean,
    skipOpen: boolean,
  },
) {
  const { hideImplicits, skipCompile, skipOpen } = options;

  const nameLookup: Record<string, string> = {};
  const implicitStructs = Object.keys(ctx).filter(x => x.startsWith(`Relt_`));

  if (hideImplicits) {
    dg = implicitStructs.reduce((p, c) => p.remove(c), dg);
  }

  const lines: Line[] = [
    line(`title: ${reltProject.name} Docs {`),
    block(
      line(`near: top-center`),
      line(`shape: text`),
      line(`style: {`),
      block(
        line(`font-size: 29`),
        line(`bold: true`),
        line(`underline: true`),
      ),
      line(`}`),
    ),
    line(`}`),
    nl,
    ...sparkProject.datasetHandlers.flatMap(ds => {
      const type = ctx[ds.output.name];

      if (type === undefined || type.kind !== 'StructType') return [];
      if (hideImplicits && implicitStructs.includes(type.name)) return [];

      if (isSparkSourceDataSet(ds)) {
        const name = type.name;

        nameLookup[name] = `${name}_outer`;

        return [
          line(`${name}_outer: ${name} {`),
          block(
            line(`${name}_text: |md`),
            block(
              ...(() => {
                switch (ds.kind) {
                  case "SparkDBSourceDatasetHandler":
                    return [
                      line(`# Database Source Type`),
                      nl,
                      line(`## Connection`),
                      nl,
                      line(`\`\`\``),
                      line(`postgresql://${ds.user}:${ds.password}@${ds.host}:${ds.port}`),
                      line(`\`\`\``),
                      line(`## Table`),
                      nl,
                      line(`\`\`\``),
                      line(`${ds.table}`),
                      line(`\`\`\``),
                    ];
                  case "SparkFileSourceDatasetHandler":
                    return [
                      line(`# File Source Type`),
                      nl,
                      line(`## Path`),
                      nl,
                      line(`\`\`\``),
                      line(`${ds.path}`),
                      line(`\`\`\``),
                      line(`## Format`),
                      nl,
                      line(`\`\`\``),
                      line(`${ds.format}`),
                      line(`\`\`\``),
                    ];
                }
              })()
            ),
            line(`|`),
            nl,
            line(`${name}_inner: ${name} {`),
            block(
              line(`shape: sql_table`),
              ...type.properties.map(p => line(`${p.name}: ${convertTypeToD2SqlType(p.type, ctx, true)}`))
            ),
            line(`}`),
          ),
          line(`}`),
          nl,
        ];
      }
      else {
        const name = type.name;

        nameLookup[type.name] = name;

        return [
          line(`${name}: {`),
          block(
            line(`shape: sql_table`),
            ...type.properties.map(p => line(`${p.name}: ${convertTypeToD2SqlType(p.type, ctx, true)}`))
          ),
          line(`}`),
          nl,
        ];
      }
    }),
    nl,
    ...dg.topologicalSort().flatMap(x => {
      const me = nameLookup[x];
      const kids = dg.childrenOf(x).map(x => nameLookup[x]);
      return [
        ...kids.map(k => line(`${me} -> ${k}`)),
        nl,
      ];
    })
  ];

  const d2Source = generateLines(lines);

  const prefix = `${reltProject.outDir}/${reltProject.name}/generated`;

  await ensureDirectoryExists(prefix);
  await emptyDirectory(prefix);

  await writeFile(`${prefix}/docs.d2`, d2Source);

  if (skipCompile) return;

  console.log("Compiling D2 file");
  try {
    spawnSync('d2', [`${prefix}/docs.d2`]);
  }
  catch (e) {
    console.log("You are probably missing d2");
    console.log(`Please see: https://github.com/terrastruct/d2#install`);
    console.log(`Then run again`);
    return;
  }

  if (skipOpen) return;

  console.log("Opening documentation");

  try {
    spawnSync('python3', ['-c', `import webbrowser; webbrowser.open('${prefix}/docs.svg')`]);
  }
  catch (e) {
    console.log(`Opening the svg failed with python ${e}`);
    return;
  }
}


