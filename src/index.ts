import yargs from "yargs";
import { compile } from "./commands/compile";
import { document } from "./commands/document";
import { init } from "./commands/init";
import { pull } from "./commands/pull";
import { run } from "./commands/run";
import { version } from "./version";

async function main() {
  let cli = yargs.scriptName("relt");
  cli = init(cli);
  cli = compile(cli);
  cli = pull(cli);
  cli = run(cli);
  cli = document(cli);
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
