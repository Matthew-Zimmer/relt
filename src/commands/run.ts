import { readSourceCode, parseSourceCode, typeCheck, writeScalaProject } from "../api/compile";
import { evaluateAllExpressions } from "../passes/evaluate";
import { deriveSparkProject } from "../passes/typedReltToScala";
import { readDefaultedReltProject } from "../project";
import { spawn } from 'child_process';
import { isSparkSourceDataSet, SparkDatasetHandler } from "../asts/scala";

export interface RunArgs {
  command: string;
  tables: string[];
  show?: string[];
}

function convertDatasetHandlers(ds: SparkDatasetHandler, shouldShow: boolean): SparkDatasetHandler {
  if (shouldShow)
    ds = { ...ds, show: true };

  if (isSparkSourceDataSet(ds))
    ds = {
      kind: "SparkFileSourceDatasetHandler",
      format: "json",
      output: ds.output,
      path: `data/${ds.output.name}.jsonl`,
      show: ds.show,
    };

  return ds;
}

export async function run(args: RunArgs) {
  let { command, tables, show } = args;

  show ??= [];

  const reltProject = await readDefaultedReltProject();
  const fileContent = await readSourceCode(reltProject);
  const topLevelExpressions = parseSourceCode(fileContent);
  const [typedExpressions, ectx, typedTypeExpressions, tctx] = typeCheck(topLevelExpressions);
  const [values, scope] = evaluateAllExpressions(typedExpressions);
  let sparkProject = deriveSparkProject(reltProject, typedTypeExpressions, ectx, scope);

  sparkProject = {
    ...sparkProject,
    datasetHandlers: sparkProject.datasetHandlers.map(ds =>
      convertDatasetHandlers(ds, show!.includes(ds.output.name))
    ),
  };

  await writeScalaProject(reltProject, sparkProject);

  console.log('Compiled Relt project, running now!');

  const job = spawn("sbt", [`run ${command} ${tables.join(' ')}`], {
    cwd: `${reltProject.outDir}/${reltProject.name}`,
  });

  job.stdout.on('data', (msg: Buffer) => {
    process.stdout.write(msg);
  });

  job.stderr.on('data', (msg) => {
    process.stderr.write(msg);
  });

  await new Promise<void>((resolve, reject) => {
    const cleanup = (f: () => any) => {
      job.removeAllListeners();
      return f();
    }
    job.on('exit', () => cleanup(resolve));
    job.on('error', (e) => cleanup(() => reject(e)));
  });
}
