import { existsSync } from 'fs';
import { readFile, writeFile } from 'fs/promises';

export const reltConfigFileName = `relt.json`;

export function isReltProject() {
  return existsSync(reltConfigFileName);
}

export interface ReltProject {
  name: string;
  package: string;
  srcDir?: string;
  mainFile?: string;
}

export function defaultedReltProject(p: ReltProject): Required<ReltProject> {
  return {
    name: p.name,
    mainFile: p.mainFile ?? 'main.relt',
    package: p.package,
    srcDir: p.srcDir ?? 'src',
  };
}

export async function createReltProject(p: ReltProject) {
  return writeFile(reltConfigFileName, JSON.stringify(p, undefined, 2));
}

export async function readReltProject(): Promise<ReltProject> {
  if (!isReltProject())
    throw `Error: Not in a relt project (did you forget to initialize the project?)`;
  return JSON.parse((await readFile(reltConfigFileName)).toString());
}

export async function readDefaultedReltProject(): Promise<Required<ReltProject>> {
  return defaultedReltProject(await readReltProject());
}
