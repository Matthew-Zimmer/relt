import { compile } from "./commands/compile";
import { initProject } from "./commands/init";
import yargs from 'yargs';

async function main() {
  yargs
    .scriptName("relt")
    .command('compile', 'Compile the relt project', {
      "developer-mode": { boolean: true, description: 'Generate relt developer checkpoints for testing', default: false },
    }, compile)
    .command('init', 'Create a new project', {
      name: { string: true, alias: 'n', description: 'The name of the project' },
      package: { string: true, alias: 'p', description: 'The name of the generated scala package' },
      srcDir: { string: true, description: 'The directory which contains the main relt file' },
      outDir: { string: true, description: 'The directory which contains the generated scala project(s)' },
      mainFile: { string: true, description: 'The name of the main relt file which the compiler starts with' },
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
