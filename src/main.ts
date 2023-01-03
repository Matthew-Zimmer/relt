import { compile } from "./commands/compile";
import { initProject } from "./commands/init";
import yargs from 'yargs';
import { pull } from "./commands/pull";
import { run } from "./commands/run";

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
    .command('pull [env] [tables..]', 'Pull in data for local testing', yargs => (
      yargs
        .positional('env', {
          type: 'string',
          describe: 'The env to pull the data from'
        })
        .demandOption('env')
        .positional('tables', {
          array: true,
          type: "string",
          describe: "The name of the tables to pull data for",
          default: [],
        })
    ), x => pull(x))
    .command('run [command] [tables..]', 'Pull in data for local testing', yargs => (
      yargs
        .positional('command', {
          type: 'string',
          choices: ["refresh"],
          describe: 'The mode of the spark job to run'
        })
        .demandOption('command')
        .positional('tables', {
          array: true,
          type: "string",
          describe: "The name of the tables which have be updated",
          default: [],
        })
        .option("show", {
          alias: "s",
          array: true,
          string: true,
          describe: "Force these tables to be shown",
        })
    ), x => run(x))
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
