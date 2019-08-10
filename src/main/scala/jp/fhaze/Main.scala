package jp.fhaze

import java.util.UUID

import jp.fhaze.validator.ValidatorFactory
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object Main extends App {
  // Create a uuid for creating temporary files
  val uuid = UUID.randomUUID().toString

  // Define directories
  val inputFile  = s"hdfs/data/example.txt"
  val tempFile   = s"hdfs/data/out/example_${uuid}.txt"
  val outputFile = s"hdfs/data/out/output.txt"
  val errorFile  = s"hdfs/data/out/error.txt"

  // Spark context
  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")
  val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = SparkContext.getOrCreate(sparkConf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  // Load example txt into a dataFrame
  val example = ss.read
    .option("header", "true")
    .option("delimiter", "|")
    .csv(inputFile)

  // Validate example txt by creating "_validated" columns with a bool containing "true" or "false"
  val validatedValues = Helper.validate(example)

  // Put all "_validated" columns into an array
  val validationFields = Helper.getValidatonFields(validatedValues)

  // Filter "Good" and "Bad" values using "_validated" columns
  val onlyGoodValues = validatedValues.filter(validationFields.map(col(_) === true).reduce(_ and _))
  val onlyBadValues  = validatedValues.filter(validationFields.map(col(_) === false).reduce(_ or _))

  // Drop all "_validated" columns and save dataset into fs
  Helper.saveDataFrameToFileSystem(onlyGoodValues.drop(validationFields: _*), outputFile)
  Helper.saveDataFrameToFileSystem(onlyBadValues.drop(validationFields: _*), errorFile)

  // Preview results
  validatedValues.show()

  object Helper extends Serializable {

    def saveDataFrameToFileSystem(df: DataFrame, destination: String) = {
      createHeaderDf(df).union(df).write
        .option("header", "false")
        .option("delimiter", "|")
        .csv(tempFile)

      val src = new Path(tempFile)
      val dst = new Path(destination)

      if (fs.exists(dst))
        fs.delete(dst, true)
      FileUtil.copyMerge(fs, src, fs, dst, true, sc.hadoopConfiguration, null)
    }

    def getValidatonFields(df: DataFrame) = {
      df.columns.filter(_.endsWith("_validated"))
    }

    def validate(df: DataFrame) = {
      val header     = getHeader(df)
      val validators = header.map(ValidatorFactory.create)

      var stage = df
      validators.foreach(validator => {
        stage = validator.validate(ss, stage)
      })

      stage
    }

    private def createHeaderDf(df: DataFrame) = {
      val header = getHeader(df)
      import scala.collection.JavaConverters._
      ss.createDataFrame(List(Row.fromSeq(header.toSeq)).asJava, df.schema)
    }

    private def getHeader(df: DataFrame) = df.schema.fields.map(_.name)
  }
}
