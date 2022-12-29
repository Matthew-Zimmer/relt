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
            line(`Row(1, "Third Bank", 100.29),`),
            line(`Row(2, "Small Shoes", 4543.35),`),
            line(`Row(1, "PaperCo", 84990.15),`),
            line(`Row(1, "Disco Balls'r'us", 504.00),`),
            line(`Row(2, "Big Shovels", 9.99)`),
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
    line(`class Vertex[T](`),
    block(
      line(`val id: Int,`),
      line(`val incoming: Array[Int],`),
      line(`val outgoing: Array[Int],`),
      line(`val data: T`),
    ),
    line(`) {}`),
    nl,
    line(`class DependencyGraph[T](`),
    block(
      line(`val vertices: Map[Int, Vertex[T]],`),
    ),
    line(`) {`),
    nl,
    line(`def familyOf(origins: Array[Int]): DependencyGraph[T] = {`),
    block(
      line(`val vs = this.resolveMany(origins)`),
      line(`var seen = Set[Int]()`),
      line(`var family = Array[Vertex[T]]()`),
      nl,
      line(`for (v <- vs) {`),
      block(
        line(`val d = this.descendants(v, seen)`),
        line(`val p = d._1.foldLeft((Array[Vertex[T]](), seen))((p, c) => {`),
        block(
          line(`val x = this.ancestors(c, p._2)`),
          line(`(p._1 ++ x._1, p._2 ++ x._2)`),
        ),
        line(`})`),
        line(`seen = p._2`),
        line(`family ++= p._1`),
      ),
      line(`}`),
      nl,
      line(`new DependencyGraph(family.map(x => (x.id, new Vertex[T](x.id, x.incoming.filter(y => seen.contains(y)), x.outgoing.filter(y => seen.contains(y)), x.data))).toMap)`),
    ),
    line(`}`),
    nl,
    line(`def topologicalSort(): Array[Vertex[T]] = {`),
    block(
      line(`val visited = scala.collection.mutable.Map[Int, Boolean](this.vertices.keys.map(k => (k, false)).toSeq :_*)`),
      line(`var stack = Seq[Int]()`),
      nl,
      line(`def imp(id: Int): Unit = {`),
      block(
        line(`visited(id) = true`),
        nl,
        line(`for (i <- this.resolve(id).outgoing if (!visited.get(i).get))`),
        block(
          line(`imp(i)`),
          nl,
        ),
        line(`stack +:= id`),
      ),
      line(`}`),
      nl,
      line(`for (id <- this.vertices.keys if (!visited.get(id).get))`),
      block(
        line(`imp(id)`),
        nl,
      ),
      line(`this.resolveMany(stack.toArray)`),
    ),
    line(`}`),
    nl,
    line(`private def resolve(id: Int) = this.vertices.get(id).get`),
    line(`private def resolveMany(ids: Array[Int]) = ids.map(this.resolve)`),
    nl,
    line(`private def children(v: Vertex[T]) = this.resolveMany(v.outgoing)`),
    line(`private def parents(v: Vertex[T]) = this.resolveMany(v.incoming)`),
    nl,
    line(`private def depthTraversal(v: Vertex[T], expand: (Vertex[T]) => Array[Vertex[T]], seen: Set[Int]): (Array[Vertex[T]], Set[Int]) = {`),
    block(
      line(`if (seen.contains(v.id)) `),
      block(
        line(`(Array(), seen) `),
        line(`else`),
        block(
          line(`expand(v).foldLeft((Array(v), seen + v.id))((p, c) => {`),
          block(
            line(`val q = this.depthTraversal(c, expand, p._2)`),
            line(`(p._1 ++ q._1, p._2 ++ q._2)`),
          ),
          line(`})`),
        ),
      ),
      line(`}`),
      nl,
      line(`private def descendants(v: Vertex[T], seen: Set[Int] = Set()) = this.depthTraversal(v, this.children, seen)`),
      line(`private def ancestors(v: Vertex[T], seen: Set[Int] = Set()) = this.depthTraversal(v, this.parents, seen)`),
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
      line(`private val dg = new DependencyGraph[DatasetHandler[${p.name}.Datasets]](Map(`),
      block(
        ...p.vertices.map(x => line(`${x.id} -> new Vertex(${x.id}, Array(${x.incoming.join(', ')}), Array(${x.outgoing.join(', ')}), ${x.name}Dataset),`))
      ),
      line('))'),
      nl,
      line(`private val nameLookup = Map[String, Int](`),
      block(
        ...p.vertices.map(x => line(`"${x.name}" -> ${x.id},`))
      ),
      line(')'),
      nl,
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
        nl,
        line(`args match {`),
        block(
          line(`case Array("refresh", types@_*) if types.length > 0 && types.foldLeft(true)(_ && this.nameLookup.contains(_\)) => this.refresh(spark, types.toArray)`),
          line(`case _ => println(s"Unknown args doing nothing: (\${args.fold("")(_ + " " +  _).trim()})")`),
        ),
        line(`}`),
      ),
      line(`}`),
      nl,
      line(`def refresh(spark: SparkSession, sourceTypes: Array[String]): Unit = {`),
      block(
        line(`import spark.implicits._`),
        nl,
        line(`var dss: ${p.name}.Datasets = (${p.types.map(x => `spark.emptyDataset[${x.caseClass.name}]`).join(', ')})`),
        nl,
        line(`this.dg.familyOf(sourceTypes.map(this.nameLookup.get(_).get)).topologicalSort().foreach(v => {`),
        block(
          line(`dss = v.data.handle(spark, dss)`),
        ),
        line(`})`),
      ),
      line(`}`),
    ),
    line(`}`),
  ];
}
