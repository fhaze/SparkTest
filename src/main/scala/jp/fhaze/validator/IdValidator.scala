package jp.fhaze.validator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.when

object IdValidator extends Validator {
  override def validate(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._
    df.withColumn("id_validated",
      when($"id" > 3, true)
        .otherwise(false)
        .cast("boolean"))
  }
}
