import { inspect } from "util";

export function throws(msg?: string): never {
  throw new Error(msg);
}

export function print(x: any) {
  console.log(inspect(x, false, null, false));
}

export function uncap(x: string): string {
  return x.length === 0 ? '' : x[0].toLowerCase() + x.slice(1);
}

export function dedup<T extends string | boolean | number>(x: T[]): T[] {
  return [...new Set(x)];
}
