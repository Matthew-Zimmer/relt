import yargs from "yargs";

/*
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
    .command('generate [resource]', 'Generate resources for this project', yargs => (
      yargs
        .positional('resource', {
          type: 'string',
          choices: ["docs"],
          describe: 'The type of resource to be generated'
        })
        .demandOption('resource')
        .option("show-all-types", {
          boolean: true,
          alias: "a",
          default: false,
          describe: "Show implicit types in the generated resource",
        })
        .option("skip-compile", {
          boolean: true,
          default: false,
          describe: "Skip any processing of the generated resource",
        })
        .option("skip-open", {
          boolean: true,
          default: false,
          describe: "Skip opening the resource after generating",
        })
    ), x => generate(x))
*/

export const version = '0.0.0';

async function main() {
  let cli = yargs.scriptName("relt");
  cli = init(cli);
  cli = compile(cli);
  cli = pull(cli);
  cli = run(cli);
  cli = generate(cli);
  cli
    .help()
    .version(version)
    .usage('$0 <cmd> [args]')
    .strictCommands()
    .showHelpOnFail(true)
    .demandCommand(1, '');
}

if (require.main === module)
  main().catch(e => console.error(e));
