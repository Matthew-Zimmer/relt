import { existsSync } from "fs";
import { mkdir, rm, writeFile } from "fs/promises";
import { readSourceCode, parseSourceCode, typeCheck } from "../api/compile";
import { isSparkSourceDataSet, SparkSourceDataSet } from "../asts/scala";
import { ForeignKeyType, IntegerType, StringType, StructType, Type } from "../asts/type";
import { evaluateAllExpressions } from "../passes/evaluate";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { readDefaultedReltProject, ReltProject } from "../project";
import { throws } from "../utils";
import { v4 as uuid4 } from 'uuid';

export interface PullArgs {
  env: string;
  tables: string[];
}

export async function pull(args: PullArgs) {
  let { env, tables } = args;

  const reltProject = await readDefaultedReltProject();
  const fileContent = await readSourceCode(reltProject);
  const topLevelExpressions = parseSourceCode(fileContent);
  const [typedExpressions, ectx, typedTypeExpressions, tctx, libs] = typeCheck(topLevelExpressions);
  const [values, scope] = evaluateAllExpressions(typedExpressions);
  const sparkProject = deriveSparkProject(reltProject, typedTypeExpressions, ectx, scope, libs);

  const sourceDatasets = sparkProject.datasetHandlers.filter(isSparkSourceDataSet);
  const sourceDatasetsToPull: [SparkSourceDataSet, StructType][] = [];

  for (const table of tables) {
    const ds = sourceDatasets.find(x => x.output.name === table);
    if (ds === undefined)
      throws(`Error: ${table} is not a source table and thus its data can not be pulled`);
    const t = tctx[table];
    if (t === undefined || t.kind !== 'StructType')
      throws(`Internal Error: unknown type ${table} even though a dataset handler was found for it`);
    sourceDatasetsToPull.push([ds, t]);
  }

  switch (env) {
    case "mock":
    case "local":
      pullMockData(reltProject, sourceDatasetsToPull);
      break;
    default:
      throws(`Error: TODO handle non local environment ${env}`);
  }
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

export async function pullMockData(reltProject: ReltProject, dss: [SparkSourceDataSet, StructType][]) {
  const data = generateMockDataFor(dss.map(x => x[1]));
  const prefix = `${reltProject.outDir}/${reltProject.name}/data`;

  await ensureDirectoryExists(prefix);
  await emptyDirectory(prefix);

  await Promise.all(data.map(d => (
    writeFile(`${prefix}/${d[0]}.jsonl`, d[1].map(x => JSON.stringify(x)).join('\n'))
  )));
}

export function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min) + min);
}

export function randFloat(scale: number): number {
  return Math.random() * scale;
}

const alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789'
export function randString(n: number): string {
  return Array.from({ length: n }).map(() => alphabet[randInt(0, alphabet.length)]).reduce((p, c) => p + c, '');
}

export function randBool(): boolean {
  return Math.random() < 0.5;
}

type StructContext = Record<string, Record<string, Type>>;

function structContext(structs: StructType[]): StructContext {
  return Object.fromEntries(structs.map(t => [t.name, Object.fromEntries(t.properties.map(p => [p.name, p.type]))]));
}

function resolveForeignKeyType(fk: ForeignKeyType, ctx: StructContext): IntegerType | StringType {
  const t = ctx[fk.table];
  if (t === undefined)
    throws(`Error: Cannot resolve fk ${fk.table}.${fk.column} since ${fk.table} was unknown`);
  const p = t[fk.column];
  if (p === undefined)
    throws(`Error: Cannot resolve fk ${fk.table}.${fk.column} since ${fk.column} was unknown in ${fk.table}`);
  switch (p.kind) {
    case "IntegerType":
    case "StringType":
      return p;
    case "PrimaryKeyType":
      return p.of;
    case "ForeignKeyType":
      return resolveForeignKeyType(p, ctx);
    case "FloatType":
    case "BooleanType":
    case "FunctionType":
    case "UnitType":
    case "UnionType":
    case "ArrayType":
    case "OptionalType":
    case "StructType":
      throws(`Error: fk ${fk.table}.${fk.column} resolved to ${p.kind}`);
  }
}

export function generateMockDataFor(types: StructType[]): [string, any[]][] {
  const idsForType = new Map<string, Map<string, { ids: (string | number)[], last: number }>>();
  const data: [string, any[]][] = [];

  const idInfoFor = (name: string, col: string) => {
    if (!idsForType.has(name))
      idsForType.set(name, new Map());
    const tMap = idsForType.get(name)!;
    if (!tMap.has(col))
      tMap.set(col, { ids: [], last: 0 });
    return [tMap.get(col)!, (x: { ids: (string | number)[], last: number }) => tMap.set(col, x)] as const;
  }

  const nextIdFor = (name: string, col: string, type: IntegerType | StringType): string | number => {
    const [info, setInfo] = idInfoFor(name, col);
    if (info.last === info.ids.length) {
      const id = type.kind === 'IntegerType' ? info.last : uuid4();
      setInfo({ ids: info.ids.concat(id), last: info.last + 1 });
      return id;
    }
    else {
      const id = info.ids[info.last];
      setInfo({ ids: info.ids, last: info.last + 1 });
      return id;
    }
  };

  const anyIdFor = (name: string, col: string, type: IntegerType | StringType): string | number => {
    const [info, setInfo] = idInfoFor(name, col);
    if (info.ids.length === 0) {
      const id = type.kind === 'IntegerType' ? 0 : uuid4();
      setInfo({ ids: [id], last: 1 });
      return id;
    }
    else {
      return info.ids[randInt(0, info.ids.length)];
    }
  };

  for (const type of types) {
    const myData: any[] = [];
    const n = randInt(10, 100);
    for (let i = 0; i < n; i++) {
      const row: any = {};
      for (const prop of type.properties) {
        const randValueForType = (t: Type): string | boolean | number | any[] | null => {
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
            case "PrimaryKeyType":
              return nextIdFor(type.name, prop.name, t.of);
            case "ForeignKeyType":
              return anyIdFor(t.table, t.column, resolveForeignKeyType(t, structContext(types)));
            case "FunctionType":
            case "UnitType":
            case "UnionType":
            case "StructType":
              throws(`Cannot generate mock data for ${t.kind}`);
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