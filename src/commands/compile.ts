import type { Argv } from "yargs";
import { CompilerApi } from "../api/compiler";
import { readDefaultedReltProject } from "../api/project";

export function compile<T>(cli: Argv<T>) {
  return (
    cli
      .command('compile', 'Compile the relt project',
        {
          "developer-mode": { boolean: true, description: 'Generate relt developer checkpoints for testing', default: false },
        },
        async args => {
          const project = await readDefaultedReltProject()
          const reltc = new CompilerApi(project);
          await reltc.compile();
        },
      )
  );
}
