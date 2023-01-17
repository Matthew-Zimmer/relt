class WhereDatasetHandler[DSS, DS <: Product : TypeTag](
  private val toDs: (DSS) => Dataset[DS],
  private val toDss: (DSS, Dataset[DS]) => DSS,
  private val toValue: (DSS) => Dataset[DS],
  private val on: (Dataset[DS]) => Column,
) extends TransformDatasetHandler[DSS] {
  override def transform(spark: SparkSession, plan: Plan, dss: DSS): DSS = {
    import spark.implicits._

    var ds = this.toValue(dss)

    val filteredDs = ds.where(this.on(ds))

    this.toDss(dss, filteredDs)
  }
}
