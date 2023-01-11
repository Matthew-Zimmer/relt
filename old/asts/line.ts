export type Line =
  | BlockLine
  | SingleLine
  | PrefixLine

export interface BlockLine {
  kind: "BlockLine";
  lines: Line[];
}

export interface SingleLine {
  kind: "SingleLine";
  value: string;
}

export interface PrefixLine {
  kind: "PrefixLine";
  prefix: string;
  lines: Line[];
}

export const line = (value: string): SingleLine => ({ kind: 'SingleLine', value });
export const nl: SingleLine = ({ kind: 'SingleLine', value: '' });
export const block = (...lines: Line[]): BlockLine => ({ kind: 'BlockLine', lines });
export const prefix = (prefix: string, lines: Line[]): PrefixLine => ({ kind: 'PrefixLine', prefix, lines });
