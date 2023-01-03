import { existsSync } from 'fs';
import { readFile, writeFile } from 'fs/promises';

export const reltConfigFileName = `relt.json`;

export function isReltProject() {
  return existsSync(reltConfigFileName);
}

export interface ReltProjectConfig {
  name: string;
  package?: string;
  srcDir?: string;
  mainFile?: string;
  outDir?: string;
  version?: string;
}

export type ReltProject = Required<ReltProjectConfig>;

export function defaultedReltProject(p: ReltProjectConfig): ReltProject {
  return {
    name: p.name,
    mainFile: p.mainFile ?? 'main.relt',
    package: p.package ?? "",
    srcDir: p.srcDir ?? 'src',
    outDir: p.outDir ?? 'out',
    version: p.version ?? '0.0.0',
  };
}

export async function createReltProject(p: ReltProjectConfig) {
  return writeFile(reltConfigFileName, JSON.stringify(p, undefined, 2));
}

export async function readReltProject(): Promise<ReltProject> {
  if (!isReltProject())
    throw `Error: Not in a relt project (did you forget to initialize the project?)`;
  return JSON.parse((await readFile(reltConfigFileName)).toString());
}

export async function readDefaultedReltProject(): Promise<ReltProject> {
  return defaultedReltProject(await readReltProject());
}
