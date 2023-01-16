import type { Argv } from "yargs";

export function pull<T>(cli: Argv<T>) {
  return (
    cli
      .command('pull [env] [tables..]', 'Pull in data for local testing',
        yargs => (
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
        ),
        () => { },
      )
  );
}
