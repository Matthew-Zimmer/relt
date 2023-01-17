package Test

// --- Standard Libraries ---
import scala.reflect.runtime.universe.{ TypeTag }
import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession, Column }
import org.apache.spark.sql.functions.{ collect_list, struct, sum, lit, udf }
import java.sql.Date
import scala.util.control._
import scala.collection.mutable.LinkedHashMap

// --- User Defined Libraries ---

// --- CORE LIBRARY CODE ---

final case class DBConnectionInfo[T <: Product: TypeTag] (
	host: String,
	port: Int,
	user: String,
	password: String,
	table: String,
	columns: Seq[String],
)

object Utils {
	def readTable[T <: Product: TypeTag](spark: SparkSession, conn: DBConnectionInfo[T]): Dataset[T] = {
	import spark.implicits._
	
		val data = if (conn.table == "person")
			Seq(
				Row(1, "Eric", "Tome"),
				Row(2, "Jennifer", "C"),
				Row(3, "Cara", "Rae")
			)
		else
			Seq(
				Row(1, "Third Bank", 100.29),
				Row(2, "Small Shoes", 4543.35),
				Row(1, "PaperCo", 84990.15),
				Row(1, "Disco Balls'r'us", 504.00),
				Row(2, "Big Shovels", 9.99)
			)
		spark.createDataFrame(
			spark.sparkContext.parallelize(data),
			Encoders.product[T].schema
		).as[T]
	}
}

class Vertex[T](
	val id: Int,
	val incoming: Array[Int],
	val outgoing: Array[Int],
	val data: T
) {}

class DependencyGraph[T](
	val vertices: Map[Int, Vertex[T]],
) {

	def familyOf(origins: Array[Int]): DependencyGraph[T] = {
		val vs = this.resolveMany(origins)
		var seen = Set[Int]()
		var family = Array[Vertex[T]]()
		
		for (v <- vs) {
			val d = this.descendants(v, seen)
			val p = d._1.foldLeft((Array[Vertex[T]](), seen))((p, c) => {
				val x = this.ancestors(c, p._2)
				(p._1 ++ x._1, p._2 ++ x._2)
			})
			seen = p._2
			family ++= p._1
		}
		
		new DependencyGraph(family.map(x => (x.id, new Vertex[T](x.id, x.incoming.filter(y => seen.contains(y)), x.outgoing.filter(y => seen.contains(y)), x.data))).toMap)
	}

  def addEdges(edges: Seq[(Int, Int)]): DependencyGraph[T] = {
		new DependencyGraph(this.vertices.toSeq.map((x) => {
      val (k, v) = x
      val incoming = v.incoming ++ edges.filter(_._2 == k).map(_._1).toArray[Int]
      val outgoing = v.outgoing ++ edges.filter(_._1 == k).map(_._2).toArray[Int]
      (k, new Vertex(v.id, incoming, outgoing, v.data))
    }).toMap)
	}
	
	def topologicalSort(): Array[Vertex[T]] = {
		val visited = scala.collection.mutable.Map[Int, Boolean](this.vertices.keys.map(k => (k, false)).toSeq :_*)
		var stack = Seq[Int]()
		
		def imp(id: Int): Unit = {
			visited(id) = true
			
			for (i <- this.resolve(id).outgoing if (!visited.get(i).get))
				imp(i)
				
			stack +:= id
		}
		
		for (id <- this.vertices.keys if (!visited.get(id).get))
			imp(id)
			
		this.resolveMany(stack.toArray)
	}
	
	private def resolve(id: Int) = this.vertices.get(id).get
	private def resolveMany(ids: Array[Int]) = ids.map(this.resolve)
	
	private def children(v: Vertex[T]) = this.resolveMany(v.outgoing)
	private def parents(v: Vertex[T]) = this.resolveMany(v.incoming)
	
	private def depthTraversal(v: Vertex[T], expand: (Vertex[T]) => Array[Vertex[T]], seen: Set[Int]): (Array[Vertex[T]], Set[Int]) = {
		if (seen.contains(v.id)) 
			(Array(), seen) 
			else
				expand(v).foldLeft((Array(v), seen + v.id))((p, c) => {
					val q = this.depthTraversal(c, expand, p._2)
					(p._1 ++ q._1, p._2 ++ q._2)
				})
		}
		
	private def descendants(v: Vertex[T], seen: Set[Int] = Set()) = this.depthTraversal(v, this.children, seen)
	private def ancestors(v: Vertex[T], seen: Set[Int] = Set()) = this.depthTraversal(v, this.parents, seen)

  def get(id: Int): T = this.resolve(id).data
}

// --- GENERATED CODE ---

object Delta {
  type State = Byte
  val created: State = 0
  val updated: State = 1
  val netural: State = 2
  val deleted: State = 3
}

trait Plan {
  val id: Int
}
case class ComputePlan (id: Int) extends Plan
case class RefreshPlan (id: Int) extends Plan
case class FetchPlan (id: Int) extends Plan
case class DeltaPlan (id: Int, from: Int, to: Int) extends Plan
case class WherePlan (id: Int, plan: Plan, on: Column) extends Plan
case class WithPlan (id: Int, left: Plan, right: Plan) extends Plan

trait PlanParser {
  def maybeParse(args: Array[String], idx: Int, ctx: Map[String, Int]): Option[(Plan, Int)]
}

object RefreshPlanParser extends PlanParser {
  def maybeParse(args: Array[String], idx: Int, ctx: Map[String, Int]): Option[(Plan, Int)] = {
    None
  }
}

object DeltaPlanParser extends PlanParser {
  def maybeParse(args: Array[String], idx: Int, ctx: Map[String, Int]): Option[(Plan, Int)] = {
    None
  }
}

object Udfs {
  private val innerJoinStateMap = Array[Delta.State](0,0,0,3,0,1,1,3,0,1,2,3,3,3,3,3)
  private val leftJoinStateMap = Array[Delta.State](0,0,0,3,0,1,1,3,0,1,2,3,3,3,3,3)
  val innerJoinStateUdf = udf((x: Delta.State, y: Delta.State) => this.innerJoinStateMap(x * 4 + y))
  val leftJoinStateUdf = udf((x: Delta.State, y: Delta.State) => this.leftJoinStateMap(x * 4 + y))
}

trait DatasetHandler[DSS] {
  def construct(spark: SparkSession, plan: Plan, dss: DSS): DSS = this.load(spark, plan, this.transform(spark, plan, this.extract(spark, plan, dss)))
  def extract(spark: SparkSession, plan: Plan, dss: DSS): DSS = dss
  def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = dss
  def load(spark: SparkSession, plan: Plan, dss: DSS): DSS = dss

  def constructPlan(id: Int, root: Int, relations: Map[Int, Map[Int, Column]], dg: DependencyGraph[DatasetHandler[DSS]], map: LinkedHashMap[Int, Plan]): LinkedHashMap[Int, Plan] = map
}

// Abstract Handlers

trait SourceDatasetHandler[DSS] extends DatasetHandler[DSS] {
  override def constructPlan(id: Int, root: Int, relations: Map[Int, Map[Int, Column]], dg: DependencyGraph[DatasetHandler[DSS]], map: LinkedHashMap[Int, Plan]): LinkedHashMap[Int, Plan] = {
    if (id == root) return map

    map += (id -> RefreshPlan(id))
  }
}

trait TransformDatasetHandler[DSS] extends DatasetHandler[DSS] {
  override def constructPlan(id: Int, root: Int, relations: Map[Int, Map[Int, Column]], dg: DependencyGraph[DatasetHandler[DSS]], map: LinkedHashMap[Int, Plan]): LinkedHashMap[Int, Plan] = {
    map += (id -> ComputePlan(id))
  }
}

// Extraction Handlers

class DeltaFileSourceDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
) extends SourceDatasetHandler[DSS] {
  override def extract(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    val ds = this.planExtract(spark, plan)
    this.toDss(dss, ds)
  }

  def planExtract(spark: SparkSession, plan: Plan): Dataset[DS] = {
    import spark.implicits._
    plan match {
      case RefreshPlan(id) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.created)).as[DS]
      case FetchPlan(id) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.netural)).as[DS]
      case DeltaPlan(id, from, to) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.updated)).as[DS]
      case WherePlan(id, plan, on) => this.planExtract(spark, plan).where(on)
      case WithPlan(id, left, right) => {
        val l = this.planExtract(spark, left)
        val r = this.planExtract(spark, right)
        l.union(r).dropDuplicates(Seq("s"))
      }
      case ComputePlan(id) => throw new Error("Cannot compute a source table")
      case _ => throw new Error(s"Unknown plan kind: ${plan}")
    }
  }
}

class MemoDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val handler: DatasetHandler[DSS],
) extends DatasetHandler[DSS] {
  override def extract(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    val ds = this.planExtract(spark, plan)
    this.toDss(dss, ds)
  }

  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    if (plan match { case ComputePlan(id) => false; case _ => true }) return dss
    this.handler.transform(spark, plan, dss)
  }

  override def load(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    if (plan match { case ComputePlan(id) => true; case _ => false }) {
      this.toDs(dss).write.format("").save("")
    }
    this.handler.load(spark, plan, dss)
  }

  def planExtract(spark: SparkSession, plan: Plan): Dataset[DS] = {
    import spark.implicits._
    plan match {
      case RefreshPlan(id) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.created)).as[DS]
      case FetchPlan(id) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.netural)).as[DS]
      case DeltaPlan(id, from, to) => spark.read.format("delta").load("loc").withColumn("_state", lit(Delta.updated)).as[DS]
      case WherePlan(id, plan, on) => this.planExtract(spark, plan).where(on)
      case WithPlan(id, left, right) => {
        val l = this.planExtract(spark, left)
        val r = this.planExtract(spark, right)
        l.union(r).dropDuplicates(Seq("s"))
      }
      case ComputePlan(id) => spark.emptyDataset[DS]
      case _ => throw new Error(s"Unknown plan kind: ${plan}")
    }
  }
}

// Transformation Handlers

class JoinDatasetHandler[DSS, DS <: Product : TypeTag, L <: Product : TypeTag, R <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toLeft: (DSS) => Dataset[L],
  private val toRight: (DSS) => Dataset[R],
  private val on: (Dataset[L], Dataset[R]) => Column,
  private val method: String,
  private val implicitDrops: Seq[String]
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    val left = this.toLeft(dss)
    val right = this.toRight(dss)
    val on = this.on(left, right)

    val leftDf = left.withColumnRenamed("_state", "l_state")
    val rightDf = right.withColumnRenamed("_state", "r_state")
    
    val ds = leftDf
      .join(rightDf, on, method)
      .withColumn("_state", method match {
        case "inner" => Udfs.innerJoinStateUdf(left.col("l_state"), right.col("r_state"))
        case "left" => Udfs.leftJoinStateUdf(left.col("l_state"), right.col("r_state"))
        case "right" => Udfs.leftJoinStateUdf(right.col("r_state"), left.col("l_state"))
      })
      .drop(this.implicitDrops ++ Seq("l_state", "r_state"): _*)
      .as[DS]

    this.toDss(dss, ds)
  }
}

class UnionDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toLeft: (DSS) => Dataset[DS],
  private val toRight: (DSS) => Dataset[DS],
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    val left = this.toLeft(dss)
    val right = this.toRight(dss)
    
    val ds = left
      .union(right)
      .as[DS]

    this.toDss(dss, ds)
  }
}

class WhereDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val on: (Dataset[DS]) => Column,
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var ds = this.toDs(dss)

    val filteredDs = ds.where(this.on(ds))

    this.toDss(dss, filteredDs)
  }
}

class DistinctDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val on: (Dataset[DS]) => Seq[String],
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var ds = this.toDs(dss)

    val filteredDs = ds.dropDuplicates(this.on(ds))

    this.toDss(dss, filteredDs)
  }
}

class DropDatasetHandler[DSS, DS <: Product : TypeTag, T <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toT: (DSS) => Dataset[T],
  private val on: (Dataset[T]) => Seq[String],
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var subDs = this.toT(dss)

    val filteredDs = subDs.drop(this.on(subDs): _*).as[DS]

    this.toDss(dss, filteredDs)
  }
}

class SelectDatasetHandler[DSS, DS <: Product : TypeTag, T <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toT: (DSS) => Dataset[T],
  private val on: (Dataset[T]) => Seq[Column],
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var subDs = this.toT(dss)

    val filteredDs = subDs.select(this.on(subDs): _*).as[DS]

    this.toDss(dss, filteredDs)
  }
}

// class DeltaAggregatedGroupDatasetHandler[DSS, DS <: Product : TypeTag, T <: Product : TypeTag](
//   private val toDs: (DSS) => Dataset[DS],
//   private val toDss: (DSS, Dataset[DS]) => DSS,
//   private val toT: (DSS) => Dataset[T],
//   private val cols: (Dataset[T]) => Seq[Column],
//   private val aggs: (Dataset[T]) => Seq[Column],
//   private val combineAggs: (DataFrame) => Seq[Column],
// ) extends DatasetHandler[DSS] {
//   override def extract(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
//     import spark.implicits._

//     // if we get a compute plan then don't load the values
//     // if we get a with (fetch, compute) load data
//     // if we get a with (where(fetch, filter), compute) load data with filter
//     // if we get a fetch load data
//     // if we get a where(fetch, filter) load data with filter

//     val prev = spark.read.format("delta").load("path").as[DS]
//     this.toDss(dss, prev)
//   }

//   override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
//     import spark.implicits._

//     // if we get a compute plan then do agg
//     // if we get a with (fetch, compute) then do agg
//     // if we get a with (where(fetch, filter), compute) then do agg
//     // if we get a fetch load data then dont do agg
//     // if we get a where(fetch, filter) then dont do agg

//     val prev = this.toDS(dss)
//     val input = this.toT(dss)
//     val cols = this.cols(input)
//     val aggs = this.aggs(input)

//     val delta = input.groupBy(cols: _*).agg(aggs: _*)

//     val combineAggs = this.combineAggs(delta)
//     val ds = prev.union(delta).groupBy(cols: _*).agg(combineAggs: _*)

//     this.toDss(dss, ds)
//   }

//   override def load(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
//     // if we get a compute plan then save
//     // if we get a with (fetch, compute) then save
//     // if we get a with (where(fetch, filter), compute) then save
//     // if we get a fetch load data then dont save
//     // if we get a where(fetch, filter) then dont save
//     val ds = this.toDS(dss)
//     ds.write.format("delta").save("path")
//   }
// }

// Load Handlers

class ShowDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
) extends DatasetHandler[DSS] {
  override def load(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    val ds = this.toDs(dss)
    ds.where(ds.col("_state") =!= lit(Delta.deleted)).show()
    dss
  }
}

package object Test {
  type Datasets = (Dataset[A], Dataset[B], Dataset[C], Dataset[D], Dataset[C], Dataset[F])
}

case class A ()
case class B ()
case class C ()
case class D ()
case class F ()

object ADatasetHandler extends DeltaFileSourceDatasetHandler[Test.Datasets, A] (
  (dss) => dss._1,
  (dss, ds) => (ds, dss._2, dss._3, dss._4, dss._5, dss._6),
) {}

object BDatasetHandler extends DeltaFileSourceDatasetHandler[Test.Datasets, B](
  (dss) => dss._2,
  (dss, ds) => (dss._1, ds, dss._3, dss._4, dss._5, dss._6),
) {}

object CDatasetHandler extends DeltaFileSourceDatasetHandler[Test.Datasets, C](
  (dss) => dss._3,
  (dss, ds) => (dss._1, dss._2, ds, dss._4, dss._5, dss._6),
) {}

object EDatasetHandler extends WhereDatasetHandler[Test.Datasets, C](
  (dss) => dss._5,
  (dss, ds) => (dss._1, dss._2, dss._3, dss._4, ds, dss._6),
  (ds) => ds.col("col") === lit("name")
) {}

object DDatasetHandler extends JoinDatasetHandler[Test.Datasets, D, A, B](
  (dss) => dss._4,
  (dss, ds) => (dss._1, dss._2, dss._3, ds, dss._5, dss._6),
  (dss) => dss._1,
  (dss) => dss._2,
  (l, r) => l.col("id") === r.col("lid"),
  "Inner",
  Seq("lid"),
) {}

object FDatasetHandler extends JoinDatasetHandler[Test.Datasets, F, D, C](
  (dss) => dss._6,
  (dss, ds) => (dss._1, dss._2, dss._3, dss._4, dss._5, ds),
  (dss) => dss._4,
  (dss) => dss._5,
  (l, r) => l.col("id") === r.col("lid"),
  "Inner",
  Seq("lid"),
) {}

object Project {
  private val dg = new DependencyGraph[DatasetHandler[Test.Datasets]](Map(
    0 -> new Vertex(0, Array(), Array(3), ADatasetHandler),
    1 -> new Vertex(1, Array(), Array(3), BDatasetHandler),
    2 -> new Vertex(2, Array(), Array(4), CDatasetHandler),
    3 -> new Vertex(3, Array(0, 1), Array(5), DDatasetHandler),
    4 -> new Vertex(4, Array(2), Array(5), EDatasetHandler),
    5 -> new Vertex(5, Array(3, 4), Array(), FDatasetHandler),
  ))
  private val sourceTables = Map[String, Int](
    "A" -> 0,
    "B" -> 1,
    "C" -> 2,
  )
  private val sourceTableRelations = Map[Int, Map[Int, Column]]()
  private val planParsers = Seq[PlanParser](
    RefreshPlanParser,
    DeltaPlanParser,
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
			.builder()
			.appName("Test")
			.master("local") // LOCAL
			.getOrCreate()
		
		spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    var requestedPlans = Seq[Plan]()
    var idx = 0
    val stop = new Breaks

    while (idx < args.size) {
      stop.breakable {
        for (parser <- this.planParsers) {
          val res = parser.maybeParse(args, idx, this.sourceTables)
          res match {
            case Some((plan, i)) => {
              idx = i
              requestedPlans :+= plan
              stop.break
            }
            case None => {}
          }
        }
        System.err.println(s"${args.slice(idx, -1).mkString(" ")} bad request")
        System.exit(1)
      }
    }

    if (requestedPlans.size == 0) {
      println("Nothing todo")
      System.exit(0)
    }
    if (requestedPlans.size > 1) {
      println("More than one source table being requested is not allowed as of now")
      System.exit(1)
    }

    val id = requestedPlans(0).id

    val dg = this.dg.addEdges(this.sourceTables.values.toSeq.map(x => (id, x))).familyOf(requestedPlans.map(_.id).toArray)

    val executionPlans = dg.topologicalSort.foldLeft(LinkedHashMap(requestedPlans.map(x => (x.id -> x)): _*))((p, c) => c.data.constructPlan(c.id, id, this.sourceTableRelations, dg, p))

    var dss = (spark.emptyDataset[A], spark.emptyDataset[B], spark.emptyDataset[C], spark.emptyDataset[D], spark.emptyDataset[C], spark.emptyDataset[F])

    for ((id, plan) <- executionPlans) {
      dss = this.dg.get(id).construct(spark, plan, dss)
    }

    println("DONE!")
  }
}
