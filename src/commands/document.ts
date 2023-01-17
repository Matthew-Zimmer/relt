import { spawnSync } from "child_process";
import { writeFile } from "fs/promises";
import type { Argv } from "yargs";
import { CompilerApi } from "../api/compiler";
import { readDefaultedReltProject, ReltProject } from "../api/project";
import { ensureDirectoryExists, emptyDirectory } from "../api/utils";
import { TableType, Type } from "../compiler/ast/relt/type";
import { TypedExpression } from "../compiler/ast/relt/typed";
import { kids, ofType } from "../compiler/ast/relt/typed/utils";

export function document<T>(cli: Argv<T>) {
  return (
    cli
      .command('generate [resource]', 'Generate resources for this project',
        yargs => (
          yargs
            .positional('resource', {
              type: 'string',
              choices: ["docs"],
              describe: 'The type of resource to be generated'
            })
            .demandOption('resource')
            .option("show-all-types", {
              boolean: true,
              alias: "a",
              default: false,
              describe: "Show implicit types in the generated resource",
            })
            .option("skip-compile", {
              boolean: true,
              default: false,
              describe: "Skip any processing of the generated resource",
            })
            .option("skip-open", {
              boolean: true,
              default: false,
              describe: "Skip opening the resource after generating",
            })
        ),
        async args => {
          const project = await readDefaultedReltProject();
          const reltc = new CompilerApi(project);
          await generateDocs(project, reltc, {
            showImplicits: args["show-all-types"],
            skipCompile: args["skip-compile"],
            skipOpen: args["skip-open"],
          });
        },
      )
  );
}

function convertTypeToD2SqlType(t: Type): string {
  const imp = (t: Type) => convertTypeToD2SqlType(t);
  switch (t.kind) {
    case "TableType":
      return t.name;
    case "ArrayType":
      return `${imp(t.of)}\\[\\]`;
    case "TupleType":
      return `\\[${t.types.map(imp).join(', ')}\\]`;
    case "BooleanType":
      return "bool";
    case "FloatType":
      return "float";
    case "FunctionType":
      return `(${imp(t.from)}) => ${imp(t.to)}`;
    case "IntegerType":
      return `int`;
    case "OptionalType":
      return `${imp(t.of)}?`;
    case "StringType":
      return `string`;
    case "UnitType":
      return 'unit';
    case "AnyType":
      return 'any';
    case "NeverType":
      return 'never';
    case "IdentifierType":
      return t.name;
    case "NullType":
      return 'null';
    case "ObjectType":
      return `\\{ ${t.properties.map(x => `${x.name}: ${imp(x.type)}`).join(', ')} \\}`;
  }
}

async function generateDocs(
  reltProject: ReltProject,
  reltc: CompilerApi,
  options: {
    showImplicits: boolean,
    skipCompile: boolean,
    skipOpen: boolean,
  },
) {
  const { showImplicits, skipCompile, skipOpen } = options;

  const step = await reltc.compile('typed-ast');

  const tables: Record<string, { show: boolean, kids: string[], type: TableType }> = {};

  const resolveShownKids = (kids: string[]): string[] => {
    const shownKids = kids.flatMap(x => tables[x].show ? [x] : resolveShownKids([x]));
    return [...new Set(shownKids)];
  }

  const implicitTableHandler = (e: TypedExpression & { type: TableType }) => {
    tables[e.type.name] = {
      show: showImplicits,
      kids: kids(e).filter(ofType('TableType')).map(x => x.type.name),
      type: e.type,
    };
  }

  step.visit({
    TypedTableExpression: e => {
      tables[e.type.name] = {
        show: true,
        kids: kids(e).filter(ofType('TableType')).map(x => x.type.name),
        type: e.type,
      };
    },
    TypedWithExpression: implicitTableHandler,
    TypedUnionExpression: implicitTableHandler,
    TypedJoinExpression: implicitTableHandler,
    TypedGroupByExpression: implicitTableHandler,
    TypedWhereExpression: implicitTableHandler,
    TypedDropExpression: implicitTableHandler,
    TypedSelectExpression: implicitTableHandler,
  });

  const d2Source = `\
title: ${reltProject.name} Docs {,
  near: top-center
  shape: text
  style: {
    font-size: 29
    bold: true
    underline: true
  }
}

${Object.entries(tables).map(([k, v]) => `${k}: {\n\tshape: sql_table${v.type.columns.map(x => `\n\t${x.name}: ${convertTypeToD2SqlType(x.type)}`).join('')}\n}`).join('\n\n')}

${Object.entries(tables).map(([k, v]) => {
    const kids = resolveShownKids(v.kids);
    return kids.map(x => `${k} -> ${x}`).join('\n') + '\n\n';
  })}
`;

  const prefix = `${reltProject.outDir}/${reltProject.name}/generated`;

  await ensureDirectoryExists(prefix);
  await emptyDirectory(prefix);

  await writeFile(`${prefix}/docs.d2`, d2Source);

  if (skipCompile) return;

  console.log("Compiling D2 file");

  const d2Res = spawnSync('d2', [`${prefix}/docs.d2`]);
  if (d2Res === null || d2Res.status !== 0) {
    console.error("Failed to compile with d2, is it installed?")
    console.error(`Please see: https://github.com/terrastruct/d2#install`);
    console.error(`Then run again, if so`);
    if (d2Res.error)
      console.error(d2Res.error.message);
    return;
  }

  if (skipOpen) return;

  console.log("Opening documentation");

  const openRes = spawnSync('python3', ['-c', `import webbrowser; webbrowser.open('file://${process.cwd()}/${prefix}/docs.svg')`]);
  if (openRes === null || openRes.status !== 0) {
    console.log(`Opening the svg failed with python`);
    if (openRes.error)
      console.error(openRes.error.message);
    return;
  }
}
