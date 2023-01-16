import type { Argv } from "yargs";

export function run<T>(cli: Argv<T>) {
  return (
    cli
      .command('run [command] [tables..]', 'Pull in data for local testing',
        yargs => (
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
        ),
        () => { },
      )
  );
}
