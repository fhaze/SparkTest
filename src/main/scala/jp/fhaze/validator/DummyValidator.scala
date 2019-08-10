package jp.fhaze.validator
import org.apache.spark.sql.{DataFrame, SparkSession}

object DummyValidator extends Validator {
  override def validate(ss: SparkSession, df: DataFrame): DataFrame = df
}
