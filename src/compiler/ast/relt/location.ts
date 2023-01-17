export interface Position {
  offset: number;
  line: number;
  column: number;
}

export interface Location {
  source: string;
  start: Position;
  end: Position;
}

export const genLoc: Location = {
  source: "genLoc",
  start: { offset: 0, column: 0, line: 0 },
  end: { offset: 0, column: 0, line: 0 },
};

export function locPath(loc: Location): string {
  if (loc === undefined) return `unknown path`;
  if (loc.source === "genLoc") return `Generated`;
  return `${loc.source}:${loc.start.line}:${loc.start.column}`;
}
