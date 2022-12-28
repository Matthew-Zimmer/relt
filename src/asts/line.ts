export type Line =
  | BlockLine
  | SingleLine

export interface BlockLine {
  kind: "BlockLine";
  lines: Line[];
}
export interface SingleLine {
  kind: "SingleLine";
  value: string;
}

export const line = (value: string): SingleLine => ({ kind: 'SingleLine', value });
export const nl: SingleLine = ({ kind: 'SingleLine', value: '' });
export const block = (...lines: Line[]): BlockLine => ({ kind: 'BlockLine', lines });
