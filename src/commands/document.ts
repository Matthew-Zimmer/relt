import type { Argv } from "yargs";

export function document<T>(cli: Argv<T>) {
  return (
    cli
      .command('generate [resource]', 'Generate resources for this project',
        yargs => (
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
        ),
        () => { },
      )
  );
}
