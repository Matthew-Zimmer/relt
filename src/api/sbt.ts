import { Serializable, spawn } from "child_process";
import { stdout } from "process";

export interface SBTRunOptions {
  path: string;
  args: string[];
}

export class SBTApi {
  async run(ops: SBTRunOptions) {
    const x = spawn('sbt', ops.args, {
      cwd: ops.path
    });

    const h = (d: Serializable) => {
      stdout.write(d.toString());
    };

    x.on('message', h);

    return new Promise<void>((resolve, reject) => {
      x.once('exit', () => {
        x.off('message', h);
        resolve();
      });
      x.once('error', e => reject(e));
    });
  }
}
