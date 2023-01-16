class GroupByDatasetHandler[DSS, DS <: Product : TypeTag, T <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toT: (DSS) => Dataset[T],
  private val cols: (Dataset[T]) => Seq[Column],
  private val aggs: (Dataset[T]) => Seq[Column],
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var subDs = this.toT(dss)

    val groupedDs = subDs.groupBy(this.cols(subDs): _*).agg(this.cols(subDs): _*).as[DS]

    this.toDss(dss, groupedDs)
  }
}

