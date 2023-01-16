import type { Argv } from "yargs";

export function compile<T>(cli: Argv<T>) {
  return (
    cli
      .command('compile', 'Compile the relt project',
        {
          "developer-mode": { boolean: true, description: 'Generate relt developer checkpoints for testing', default: false },
        },
        () => { },
      )
  );
}
