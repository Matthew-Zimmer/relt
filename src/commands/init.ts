import type { Argv } from "yargs";
import { createReltProject } from "../api/project";

export function init<T>(cli: Argv<T>) {
  return (
    cli
      .command('init', 'Create a new project',
        {
          name: { string: true, alias: 'n', description: 'The name of the project', required: true },
          package: { string: true, alias: 'p', description: 'The name of the generated scala package' },
          srcDir: { string: true, description: 'The directory which contains the main relt file' },
          outDir: { string: true, description: 'The directory which contains the generated scala project(s)' },
          mainFile: { string: true, description: 'The name of the main relt file which the compiler starts with' },
        },
        async args => {
          await createReltProject(args)
        }
      )
  );
}
