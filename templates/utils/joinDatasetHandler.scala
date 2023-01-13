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
