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

export function locPath(loc: Location): string {
  return `${loc.source}:${loc.start.line}:${loc.start.column}`;
}
