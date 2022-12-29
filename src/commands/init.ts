import { createInterface, Interface } from 'readline';
import { isReltProject, createReltProject } from '../project';

export interface InitCommandArgs {
  name?: string;
  package?: string;
  srcDir?: string;
  outDir?: string;
  mainFile?: string;
}

async function question(rl: Interface, query: string): Promise<string> {
  return new Promise(resolve => {
    rl.question(query, x => resolve(x));
  });
}

export async function initProject(args: InitCommandArgs) {
  if (isReltProject())
    throw `Cannot create a project (already in a relt project)`;
  const rl = createInterface(process.stdin, process.stdout);

  if (args.name === undefined || args.package === undefined)
    console.log(`Create a new relt project!`);

  const name = args.name ?? await question(rl, 'Project Name: ');

  rl.close();

  return createReltProject({ ...args, name });
}
