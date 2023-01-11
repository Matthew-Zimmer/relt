import { SparkProject } from "../ast/scala";

export function toScala(p: SparkProject): string {
  return (
    `
package ${p.package}

// imports

// utils

trait Plan {}

case class RefreshPlan extends Plan ()
case class ComputePlan extends Plan ()
case class UpdatePlan extends Plan (
  previous: Int,
  current: Int,
)

// types

case class NAME (
  NAME: TYPE,
)

// dataset handlers

package object {
  type Datasets = ()
}

object NAME#Dataset :: memo {
  def construct(spark: SparkSession, dss: ${p.name}.Datasets, plan: Plan): ${p.name}.Datasets = {
    
  }
  def read(spark: SparkSession, dss: ${p.name}.Datasets, plan: Plan) {
    val (ds0, ds1) = (dss._1, dss._2)
    if (ds0.isEmpty && ds1.isEmpty)
      
  }
  def compute(spark: SparkSession, dss: ${p.name}.Datasets) {
    val (ds0, ds1) = (dss._1, dss._2)

  }
  def write(spark: SparkSession) {

  }
}

object ${p.name} {
  private val dg = new DependencyGraph(new Map(

  ))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
			.builder()
			.appName("${p.name}")
			.master("local") // LOCAL
			.getOrCreate()
		
		spark.sparkContext.setLogLevel("WARN")

    val plan = ?

    // resolve plan!

    // preform plan!
  }
}`
  );
}