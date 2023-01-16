object _NAME_WithDatasetHandler extends DatasetHandler[_PROJECT_.Datasets] {
  override def transform(spark: SparkSession, plan: Plan, dss: _PROJECT_.Datasets): _PROJECT_.Datasets = {
    val lDs = (_TO_L_DS_)(dss)

    val ds = lDs_EXPRESSIONS_.as[_TYPE_]

    (_TO_DSS_)(dss, ds)
  }
}

