import { Line } from '../asts/line';

export function generateLine(l: Line): string {
  let indentation = '';
  function imp(l: Line): string {
    switch (l.kind) {
      case 'BlockLine': {
        indentation += '\t';
        const lines = l.lines.map(imp).join('');
        indentation = indentation.slice(0, -1);
        return lines;
      }
      case 'SingleLine':
        return `${indentation}${l.value}\n`;
    }
  }
  return imp(l);
}

export function generateLines(lines: Line[]): string {
  return lines.map(generateLine).join('');
}
