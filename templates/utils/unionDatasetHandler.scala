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
