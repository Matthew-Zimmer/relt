package _PACKAGE_

// --- Standard Libraries ---
import scala.reflect.runtime.universe.{ TypeTag }
import org.apache.spark.sql.{ Encoders, Dataset, Row, SparkSession, Column }
import org.apache.spark.sql.functions.{ collect_list, struct, sum, lit, udf }
import java.sql.Date
import scala.util.control._
import scala.collection.mutable.LinkedHashMap

// --- User Defined Libraries ---

// --- CORE LIBRARY CODE ---

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

