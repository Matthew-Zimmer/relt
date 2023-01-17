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
