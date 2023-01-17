import { existsSync } from "fs";
import { rm, mkdir } from 'fs/promises';

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
