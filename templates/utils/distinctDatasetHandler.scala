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
