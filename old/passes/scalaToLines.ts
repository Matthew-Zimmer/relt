import { nl, line, block, Line } from '../asts/line';
import { ScalaType, ScalaCaseClass, SparkProject, SparkDatasetHandler, DatasetId, SparkMapRule, SparkAggregation, SparkExpression } from '../asts/scala';

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
    case 'ScalaUnitType':
      return 'Unit';
    case 'ScalaStringType':
      return 'String';
    case 'ScalaArrayType':
      return `Array[${generateScalaType(t.of)}]`;
    case 'ScalaOptionalType':
      return `Option[${generateScalaType(t.of)}]`;
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

export function generateSparkMapRule(r: SparkMapRule): Line {
  switch (r.kind) {
    case "SparkIdentityRule":
      return line(`${r.name}`);
    case "SparkApplicationRule":
      return line(`val ${r.name} = ${r.func}(${r.args.join(', ')})`);
    case "SparkBinaryOperationRule":
      return line(`val ${r.name} = ${r.left} ${r.op} ${r.right}`);
    case "SparkGetOrElseRule":
      return line(`val ${r.name} = ${r.left}.getOrElse(${r.right})`);
    case "SparkDotRule":
      return line(`val ${r.name} = ${r.left}.${r.right}`);
  }
}

export function generateSparkAggregation(r: SparkAggregation): Line {
  switch (r.kind) {
    case "SparkCollectListAggregation":
      return line(`collect_list(struct(${r.columns.map(x => `ds0.col("${x}")`).join(', ')})) as "${r.name}",`);
    case "SparkSqlAggregation":
      return line(`${r.func}(ds0.col("${r.column}")) as "${r.name}",`);
  }
}

export function generateDatasetHandler(h: SparkDatasetHandler, packageName: string, count: number): Line[] {
  const dssTuple = `(${Array.from({ length: count }).map((_, i) => i === h.output.idx ? `ds` : `dss._${i + 1}`).join(', ')})`;

  const derivedHandler = (inputs: DatasetId[], construct: Line[]) => {
    return [
      line(`object ${h.output.name}Dataset extends DatasetHandler[${packageName}.Datasets] {`),
      block(
        line(`def handle(spark: SparkSession, dss: ${packageName}.Datasets): ${packageName}.Datasets = {`),
        block(
          line(`val ds = this.construct(spark, ${inputs.map(x => `dss._${x.idx + 1}`).join(', ')})`),
          line(`this.write(spark, ds)`),
          line(dssTuple),
        ),
        line(`}`),
        nl,
        line(`def construct(spark: SparkSession, ${inputs.map((x, i) => `ds${i}: Dataset[${x.name}]`).join(', ')}): Dataset[${h.output.name}] = {`),
        block(
          line(`import spark.implicits._`),
          nl,
          ...construct
        ),
        line(`}`),
        nl,
        line(`def write(spark: SparkSession, ds: Dataset[${h.output.name}]): Unit = {`),
        block(
          h.show ? line(`ds.show()`) : line(`// no-op`),
        ),
        line(`}`),
      ),
      line(`}`),
      nl,
    ];
  };

  const sourceHandler = (read: Line[]) => {
    return [
      line(`object ${h.output.name}Dataset extends DatasetHandler[${packageName}.Datasets] {`),
      block(
        line(`def handle(spark: SparkSession, dss: ${packageName}.Datasets): ${packageName}.Datasets = {`),
        block(
          line(`val ds = this.read(spark)`),
          line(`this.write(spark, ds)`),
          line(dssTuple),
        ),
        line(`}`),
        nl,
        line(`def read(spark: SparkSession): Dataset[${h.output.name}] = {`),
        block(
          line(`import spark.implicits._`),
          nl,
          ...read
        ),
        line(`}`),
        nl,
        line(`def write(spark: SparkSession, ds: Dataset[${h.output.name}]): Unit = {`),
        block(
          h.show ? line(`ds.show()`) : line(`// no-op`),
        ),
        line(`}`),
      ),
      line(`}`),
      nl,
    ];
  };

  switch (h.kind) {
    case "SparkDBSourceDatasetHandler":
      return sourceHandler([
        line(`Utils.readTable[${h.output.name}](spark, DBConnectionInfo("${h.host}", ${h.port}, "${h.user}", "${h.password}", "${h.table}", Seq()))`)
      ]);
    case "SparkFileSourceDatasetHandler":
      return sourceHandler([
        line(`spark.read.format("${h.format}").schema(Encoders.product[${h.output.name}].schema).load("${h.path}").as[${h.output.name}]`)
      ]);
    case "SparkJoinDatasetHandler":
      return derivedHandler([h.leftInput, h.rightInput], [
        line(`ds0.join(ds1, ds0.col("${h.leftColumn}") === ds1.col("${h.rightColumn}"), "${h.method}").drop(ds1.col("${h.rightColumn}")).as[${h.output.name}]`)
      ]);
    case "SparkUnionDatasetHandler":
      return derivedHandler([h.leftInput, h.rightInput], [
        line(`ds0.union(ds1).as[${h.output.name}]`)
      ]);
    case "SparkDropDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.drop(${h.properties.map(x => `"${x}"`).join(', ')}).as[${h.output.name}]`),
      ]);
    case "SparkMapDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.map(row => {`),
        block(
          ...h.rules.map(generateSparkMapRule)
        ),
        line('})')
      ]);
    case "SparkGroupDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.groupBy(ds0.col("${h.column}")).agg(`),
        block(
          ...h.aggregations.map(generateSparkAggregation)
        ),
        line(`).as[${h.output.name}]`),
      ]);
    case "SparkSortDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.sort(${h.columns.map(generateSparkExpression).join(", ")}).as[${h.output.name}]`),
      ]);
    case "SparkFilterDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.filter(${generateSparkExpression(h.condition)}).as[${h.output.name}]`),
      ]);
    case "SparkDistinctDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.dropDuplicates(${h.columns.map(generateSparkExpression).join(', ')}).as[${h.output.name}]`),
      ]);
    case "SparkRepartitionDatasetHandler":
      return derivedHandler([h.input], [
        line(`ds0.repartition(${h.count}).as[${h.output.name}]`),
      ]);
  }
}

export function generateSparkExpression(e: SparkExpression): string {
  switch (e.kind) {
    case "SparkIntegerExpression":
      return `${e.value}`;
    case "SparkBooleanExpression":
      return `${e.value}`;
    case "SparkFloatExpression":
      return `${e.value.toFixed(20)}`;
    case "SparkStringExpression":
      return `"${e.value}"`;
    case "SparkIdentifierExpression":
      return e.name;
    case "SparkBinaryOperatorExpression":
      return `(${generateSparkExpression(e.left)}${e.op}${generateSparkExpression(e.right)})`;
    case "SparkLeftUnaryOperatorExpression":
      return `(${generateSparkExpression(e.left)}${e.op})`;
    case "SparkRightUnaryOperatorExpression":
      return `(${e.op}${generateSparkExpression(e.right)})`;
    case "SparkApplicationExpression":
      return `(${generateSparkExpression(e.func)}(${e.args.map(generateSparkExpression).join(", ")}))`
  }
}

export function generateSparkProject(p: SparkProject): Line[] {
  return [
    line(`package ${p.package === '' ? '' : `${p.package}.`}${p.name}`),
    nl,
    line(`// --- Standard Libraries ---`),
    line(`import scala.reflect.runtime.universe.{ TypeTag }`),
    line(`import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession }`),
    line(`import org.apache.spark.sql.functions.{ collect_list, struct, sum }`),
    line(`import java.sql.Date`),
    nl,
    line(`// --- User Defined Libraries ---`),
    ...p.libraries.map(lib => line(`import ${lib.package}.{ ${lib.name} }`)),
    nl,
    line(`// --- CORE LIBRARY CODE ---`),
    nl,
    line(`final case class DBConnectionInfo[T <: Product: TypeTag] (`),
    block(
      line(`host: String,`),
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
    block(
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
    ),
    line(`}`),
    nl,
    line(`// --- GENERATED CODE ---`),
    line(`// All datasets in the project`),
    nl,
    line(`package object ${p.name} {`),
    block(
      line(`type Datasets = (${p.datasetHandlers.map(t => `Dataset[${t.output.name}]`).join(', ')})`),
    ),
    line(`}`),
    nl,
    line(`// case classes`),
    nl,
    ...p.caseClasses.flatMap(generateCaseClass),
    line(`// dataset handlers`),
    nl,
    ...p.datasetHandlers.flatMap(h => generateDatasetHandler(h, p.name, p.datasetHandlers.length)),
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
        line(`var dss: ${p.name}.Datasets = (${p.datasetHandlers.map(t => `spark.emptyDataset[${t.output.name}]`).join(', ')})`),
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