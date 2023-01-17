import { writeFile } from "fs/promises";
import { ReltProject } from "../../api/project";
import { ensureDirectoryExists, emptyDirectory } from "../../api/utils";
import { SparkProject } from "../ast/scala";

export async function writeSparkProject(reltProject: ReltProject, sparkProject: SparkProject) {
  const projectOutDir = `${reltProject.outDir}/${reltProject.name}`;
  const packageDir = reltProject.package.split('.').join('/');

  await Promise.all([
    ensureDirectoryExists(`${projectOutDir}/project`),
    ensureDirectoryExists(`${projectOutDir}/src/main/scala/${packageDir}`),
  ]);

  await Promise.all([
    emptyDirectory(`${projectOutDir}/project`),
    emptyDirectory(`${projectOutDir}/src/main/scala/${packageDir}`),
  ]);

  await Promise.all([
    writeFile(`${projectOutDir}/project/build.properties`, `sbt.version = 1.8.0\n`),
    writeFile(`${projectOutDir}/build.sbt`, `
name := "${sparkProject.name}"

version := "${reltProject.version}"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % "2.12.0",
  "org.apache.spark" %% "spark-core" % "3.0.1",
  "org.apache.spark" %% "spark-sql" % "3.0.1",
  "org.apache.spark" %% "spark-mllib" % "3.0.1",
)\n`),
    //${sparkProject.libraries.map(lib => ("${lib.package}" %% "${lib.name}" %% "${lib.version}"))}
    writeFile(`${projectOutDir}/src/main/scala/${packageDir}/${reltProject.name}.scala`, sparkProject.sourceCode),
  ]);
}
