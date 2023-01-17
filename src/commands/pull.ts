import { writeFile } from "fs/promises";
import type { Argv } from "yargs";
import { CompilerApi, toTypedExpression } from "../api/compiler";
import { readDefaultedReltProject, ReltProject } from "../api/project";
import { ensureDirectoryExists, emptyDirectory } from "../api/utils";
import { IntegerType, StringType, TableType, Type } from "../compiler/ast/relt/type";
import { TypedTableExpression } from "../compiler/ast/relt/typed";
import { reportInternalError, reportUserError } from "../compiler/errors";

export function pull<T>(cli: Argv<T>) {
  return (
    cli
      .command('pull [env] [tables..]', 'Pull in data for local testing',
        yargs => (
          yargs
            .positional('env', {
              type: 'string',
              describe: 'The env to pull the data from'
            })
            .demandOption('env')
            .positional('tables', {
              array: true,
              type: "string",
              describe: "The name of the tables to pull data for",
              default: [] as string[],
            })
        ),
        async args => {
          const project = await readDefaultedReltProject();
          const reltc = new CompilerApi(project);
          const step = await reltc.compile("typed-ast");
          const tables: TypedTableExpression[] = [];
          step.visit({
            TypedTableExpression: e => {
              if (args.tables.includes(e.type.name))
                tables.push(e);
            }
          });
          if (args.env !== "mock" && args.env !== "local")
            reportInternalError(`TODO: pull ${args.env}`);
          // TODO?? 
        },
      )
  );
}

async function pullMockData(reltProject: ReltProject, tables: TableType[]) {
  const data = generateMockDataFor(tables);
  const prefix = `${reltProject.outDir}/${reltProject.name}/data`;

  await ensureDirectoryExists(prefix);
  await emptyDirectory(prefix);

  await Promise.all(data.map(d => (
    writeFile(`${prefix}/${d[0]}.jsonl`, d[1].map(x => JSON.stringify(x)).join('\n'))
  )));
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min) + min);
}

function randFloat(scale: number): number {
  return Math.random() * scale;
}

const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789'
function randString(n: number): string {
  return Array.from({ length: n }).map(() => alphabet[randInt(0, alphabet.length)]).reduce((p, c) => p + c, '');
}

function randBool(): boolean {
  return Math.random() < 0.5;
}

function generateMockDataFor(types: TableType[]): [string, any[]][] {
  const data: [string, any[]][] = [];

  for (const type of types) {
    const myData: any[] = [];
    const n = randInt(10, 100);
    for (let i = 0; i < n; i++) {
      const row: any = {};
      for (const prop of type.columns) {
        const randValueForType = (t: Type): string | boolean | number | any[] | null | object => {
          switch (t.kind) {
            case "IntegerType":
              return randInt(-50, 50);
            case "FloatType":
              return randFloat(10);
            case "BooleanType":
              return randBool();
            case "StringType":
              return randString(randInt(0, 16));
            case "OptionalType":
              return randBool() ? randValueForType(t.of) : null;
            case "ArrayType":
              return Array.from({ length: randInt(0, 10) }).map(x => randValueForType(t));
            case "NullType":
              return null;
            case "TupleType":
              return t.types.map(x => randValueForType(x));
            case "ObjectType":
              return Object.fromEntries(t.properties.map(x => [x.name, randValueForType(x.type)]));
            case "TableType":
            case "FunctionType":
            case "UnitType":
            case "AnyType":
            case "IdentifierType":
            case "NeverType":
              reportUserError(`Cannot generate mock data for ${t.kind}`);
          }
        }
        row[prop.name] = randValueForType(prop.type);
      }
      myData.push(row);
    }
    data.push([type.name, myData]);
  }

  return data;
}