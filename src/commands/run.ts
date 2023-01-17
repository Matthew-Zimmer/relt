import type { Argv } from "yargs";
import { CompilerApi } from "../api/compiler";
import { readDefaultedReltProject } from "../api/project";
import { SBTApi } from "../api/sbt";
import { genLoc } from "../compiler/ast/relt/location";

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
        async args => {
          const project = await readDefaultedReltProject();
          const reltc = new CompilerApi(project);
          const step = await reltc.compile('ast');
          step.transform({
            TableExpression: e => {
              const name = e.value.kind === "AssignExpression" && e.value.left.kind === "IdentifierExpression" ? e.value.left.name : undefined;
              if (name === undefined || !args.show?.includes(name)) return e;
              return {
                ...e, hooks: [
                  {
                    kind: "IdentifierExpression",
                    loc: genLoc,
                    name: "show",
                  },
                  ...e.hooks
                ]
              };
            }
          })
          const sbt = new SBTApi();
          await sbt.run({
            path: `${project.outDir}/${project.name}`,
            args: args.tables.flatMap(x => [args.command, x]),
          });
        },
      )
  );
}
