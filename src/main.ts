import { compile } from "./commands/compile";
import { initProject } from "./commands/init";
import yargs from 'yargs';

async function main() {
  yargs
    .scriptName("relt")
    .command('compile', 'Compile the relt project', {

    }, compile)
    .command('init', 'Create a new project', {
      name: { string: true, alias: 'n', description: 'The name of the project' },
      package: { string: true, alias: 'p', description: 'The name of the generated scala package' }
    }, initProject)
    .help()
    .version('0.0.0')
    .usage('$0 <cmd> [args]')
    .strictCommands()
    .showHelpOnFail(true)
    .demandCommand(1, '')
    .argv
}

if (require.main === module)
  main().catch(e => console.error(e));
