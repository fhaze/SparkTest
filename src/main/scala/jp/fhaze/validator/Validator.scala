package jp.fhaze.validator

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Validator {
  def validate(ss: SparkSession, df: DataFrame): DataFrame
}
