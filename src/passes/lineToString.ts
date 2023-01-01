import { line, Line, prefix } from '../asts/line';

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
      case 'PrefixLine':
        return l.lines.map(x => {
          switch (x.kind) {
            case "BlockLine": return x;
            case "PrefixLine": return prefix(l.prefix + x.prefix, x.lines);
            case "SingleLine": return line(l.prefix + x.value);
          }
        }).map(imp).join('');
    }
  }
  return imp(l);
}

export function generateLines(lines: Line[]): string {
  return lines.map(generateLine).join('');
}
