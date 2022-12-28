import { compile } from "./commands/compile";

async function main(args: string[]) {
  const [fileName] = args;
  await compile(fileName);
}

if (require.main === module)
  main(process.argv.slice(2)).catch(e => console.error(e));
