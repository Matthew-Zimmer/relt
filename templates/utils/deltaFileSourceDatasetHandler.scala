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

