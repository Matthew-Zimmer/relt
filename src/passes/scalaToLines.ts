import { nl, line, block, Line } from '../asts/line';
import { DatasetHandler, DerivedDatasetHandler, ScalaCaseClass, ScalaType, SourceDatasetHandler, SparkConnectionInfo, SparkMapTransformation, SparkProject, SparkRule, SparkType } from '../asts/scala';
import { uncap } from '../utils';

export function generateScalaType(t: ScalaType): string {
  switch (t.kind) {
    case 'ScalaBooleanType':
      return 'Bool';
    case 'ScalaDateType':
      return 'Date';
    case 'ScalaDoubleType':
      return 'Double';
    case 'ScalaIdentifierType':
      return t.name;
    case 'ScalaIntType':
      return 'Int';
    case 'ScalaStringType':
      return 'String';
  }
}

export function generateCaseClass(t: ScalaCaseClass): Line[] {
  return [
    line(`final case class ${t.name} (`),
    block(
      ...t.properties.map(p => line(`${p.name}: ${generateScalaType(p.type)},`)),
    ),
    line(`)`),
    nl,
  ];
}

export function generateSparkTransformation(t: SparkMapTransformation): Line[] {
  switch (t.kind) {
    case 'SparkRowExtractTransformation':
      return [line(`val ${t.name} = row.${t.property}`)];
    case 'SparkApplicationTransformation':
      return [line(`val ${t.name} = ${t.func}(${t.args.join(', ')})`)];
    case 'SparkReturnTransformation':
      return [line(`${t.name}`)];
  }
}

export function generateSparkRule(r: SparkRule): Line[] {
  switch (r.kind) {
    case 'SparkAsRule':
      return [line(`val ${r.name} = ${r.dataset}.as[${r.type}]`)];
    case 'SparkJoinRule':
      return [line(`val ${r.name} = ${r.left}.join(${r.right}, ${r.left}.col("${r.leftColumn}") === ${r.right}.col("${r.rightColumn}"), "${r.type}")`)];
    case 'SparkMapRule':
      return [
        line(`val ${r.name} = ${r.dataset}.map(row => {`),
        block(
          ...r.transformations.flatMap(generateSparkTransformation)
        ),
        line('})')
      ];
    case 'SparkIdentityRule':
      return [];
    case 'SparkReturnRule':
      return [line(`${r.name}`)];
  }
}

export function generateDerivedDatasetHandler(h: DerivedDatasetHandler, packageName: string): Line[] {
  return [
    line(`object ${h.typeName}Dataset extends DatasetHandler[${packageName}.Datasets] {`),
    block(
      line(`def handle(spark: SparkSession, dss: ${packageName}.Datasets): ${packageName}.Datasets = {`),
      block(
        line(`val ds = this.construct(spark, ${h.parentDatasets.map(x => `dss._${x.index + 1}`).join(', ')})`),
        line(`this.write(spark, ds)`),
        line(`(${Array.from({ length: h.datasetCount }).map((_, i) => i === h.datasetIndex ? `ds` : `dss._${i + 1}`).join(', ')})`),
      ),
      line(`}`),
      nl,
      line(`def construct(spark: SparkSession${h.parentDatasets.map(x => `, ${uncap(x.name)}DS: Dataset[${x.name}]`).join('')}): Dataset[${h.typeName}] = {`),
      block(
        line(`import spark.implicits._`),
        nl,
        ...h.rules.flatMap(generateSparkRule),
      ),
      line(`}`),
      nl,
      line(`def write(spark: SparkSession, ds: Dataset[${h.typeName}]): Unit = {`),
      block(
        line(`// noop`),
      ),
      line(`}`),
    ),
    line(`}`),
  ];
}

export function generateSourceDatasetHandler(h: SourceDatasetHandler, packageName: string): Line[] {
  return [
    line(`object ${h.typeName}Dataset extends DatasetHandler[${packageName}.Datasets] {`),
    block(
      line(`def handle(spark: SparkSession, dss: ${packageName}.Datasets): ${packageName}.Datasets = {`),
      block(
        line(`import spark.implicits._`),
        nl,
        line(`val ds = this.read(spark)`),
        line(`(${Array.from({ length: h.datasetCount }).map((_, i) => i === h.datasetIndex ? `ds` : `dss._${i + 1}`).join(', ')})`),
      ),
      line(`}`),
      nl,
      line(`def read(spark: SparkSession): Dataset[${h.typeName}] = {`),
      block(
        line(`import spark.implicits._`),
        nl,
        ...generateSparkConnectionInfo(h.connectionInfo, h.typeName),
      ),
      line(`}`)
    ),
    line(`}`),
  ];
}

export function generateSparkConnectionInfo(i: SparkConnectionInfo, typeName: string): Line[] {
  switch (i.kind) {
    case "SparkDBConnectionInfo":
      // TODO Seq(${i.columns.map(x => `"${x}"`).join(', ')})
      return [line(`Utils.readTable(spark, DBConnectionInfo[${typeName}]("${i.host}", ${i.port}, "${i.user}", "${i.password}", "${i.table}", Seq()))`)];
  }
}

export function generateDatasetHandler(h: DatasetHandler, packageName: string): Line[] {
  switch (h.kind) {
    case 'DerivedDatasetHandler': return generateDerivedDatasetHandler(h, packageName);
    case 'SourceDatasetHandler': return generateSourceDatasetHandler(h, packageName);
  }
}

export function generateSparkType(t: SparkType, packageName: string): Line[] {
  return [
    ...generateCaseClass(t.caseClass),
    ...generateDatasetHandler(t.datasetHandler, packageName),
    nl,
  ];
}

export function generateSparkProject(p: SparkProject): Line[] {
  return [
    line(`package ${p.name}`),
    nl,
    line(`import scala.reflect.runtime.universe.{ TypeTag }`),
    line(`import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession }`),
    line(`import java.sql.Date`),
    nl,
    line(`// --- CORE LIBRARY CODE ---`),
    nl,
    line(`object Ops {`),
    block(
      line(`def concat(l: String, r: String): String = l + " " + r`),
    ),
    line(`}`),
    nl,
    line(`final case class DBConnectionInfo[T <: Product: TypeTag] (`),
    block(
      line(`url: String,`),
      line(`port: Int,`),
      line(`user: String,`),
      line(`password: String,`),
      line(`table: String,`),
      line(`columns: Seq[String],`),
    ),
    line(`)`),
    nl,
    line(`object Utils {`),
    block(
      line(`def readTable[T <: Product: TypeTag](spark: SparkSession, conn: DBConnectionInfo[T]): Dataset[T] = {`),
      line(`import spark.implicits._`),
      nl,
      block(
        line(`val data = if (conn.table == "person")`),
        block(
          line(`Seq(`),
          block(
            line(`Row(1, "Eric", "Tome"),`),
            line(`Row(2, "Jennifer", "C"),`),
            line(`Row(3, "Cara", "Rae")`),
          ),
          line(`)`),
        ),
        line(`else`),
        block(
          line(`Seq(`),
          block(
            line(`Row(new Date(1577858400000L), 1, "Third Bank", 100.29),`),
            line(`Row(new Date(1585717200000L), 2, "Small Shoes", 4543.35),`),
            line(`Row(new Date(1593579600000L), 1, "PaperCo", 84990.15),`),
            line(`Row(new Date(1601528400000L), 1, "Disco Balls'r'us", 504.00),`),
            line(`Row(new Date(1601528400000L), 2, "Big Shovels", 9.99)`),
          ),
          line(`)`),
        ),
        line(`spark.createDataFrame(`),
        block(
          line(`spark.sparkContext.parallelize(data),`),
          line(`Encoders.product[T].schema`),
        ),
        line(`).as[T]`)
      ),
      line(`}`),
    ),
    line(`}`),
    nl,
    line(`trait DatasetHandler[T] {`),
    block(
      line(`def handle(spark: SparkSession, dss: T): T;`),
    ),
    line(`}`),
    nl,
    line(`class DependencyGraph[T] {`),
    block(
      line(`def nodes(sourceTypes: Array[String]): Array[DatasetHandler[T]] = {`),
      block(
        line(`Array()`)
      ),
      line(`}`),
    ),
    line(`}`),
    nl,
    line(`// --- GENERATED CODE ---`),
    nl,
    line(`// All datasets in the project`),
    nl,
    line(`package object ${p.name} {`),
    block(
      line(`type Datasets = (${p.types.map(t => `Dataset[${t.caseClass.name}]`).join(', ')})`),
    ),
    line(`}`),
    nl,
    line(`// spark types`),
    nl,
    ...p.types.flatMap(t => generateSparkType(t, p.name)),
    line(`// Runtime Project`),
    nl,
    line(`object PROJECT {`),
    block(
      line(`def main(args: Array[String]): Unit = {`),
      block(
        line(`val spark = SparkSession`),
        block(
          line(`.builder()`),
          line(`.appName("PROJECT")`),
          line(`.master("local") // LOCAL`),
          line(`.getOrCreate()`),
        ),
        nl,
        line(`spark.sparkContext.setLogLevel("WARN")`),
      ),
      line(`}`),
      nl,
      line(`//def refresh(spark: SparkSession, sourceTypes: Array[String]): Unit = {`),
      block(
        line(`//var dss: Datasets = (spark.emptyDataset[Person], spark.emptyDataset[Sale], spark.emptyDataset[PersonSale])`),
        line(`//DependencyGraph.nodes().forEach(handler => {`),
        block(
          line(`//dss = handler.handle(spark, dss)`),
        ),
        line(`//})`),
      ),
      line(`//}`),
    ),
    line(`}`),
  ];
}
