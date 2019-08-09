package jp.fhaze

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  val uuid = UUID.randomUUID().toString

  val inputFile  = s"hdfs/data/example.txt"
  val tempFile   = s"hdfs/data/out/example_${uuid}.txt"
  val outputFile = s"hdfs/data/out/output.txt"
  val errorFile  = s"hdfs/data/out/error.txt"

  val sparkConf = new SparkConf().setMaster("local[4]").setAppName("SparkTest")
  val ss = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = SparkContext.getOrCreate(sparkConf)
  val fs = FileSystem.get(sc.hadoopConfiguration)

  import ss.implicits._

  val example = ss.read
    .option("header", "true")
    .option("delimiter", "|")
    .csv(inputFile)

  val validatedValues = Helper.validate(example)
  val onlyGoodValues  = validatedValues.filter($"all_validated" === true)
  val onlyBadValues   = validatedValues.filter($"all_validated" === false)

  Helper.saveDataFrameToFileSystem(onlyGoodValues.drop("all_validated", "sex_validated", "id_validated"), outputFile)
  Helper.saveDataFrameToFileSystem(onlyBadValues.drop("all_validated", "sex_validated", "id_validated"), errorFile)

  validatedValues.show()

  object Helper extends Serializable {

    def saveDataFrameToFileSystem(df: DataFrame, destination: String) = {
      createHeaderDf(df).union(df).write
        .option("header", "false")
        .option("delimiter", "|")
        .csv(tempFile)

      val src = new Path(tempFile)
      val dst = new Path(destination)

      FileUtil.copyMerge(fs, src, fs, dst, true, sc.hadoopConfiguration, null)
    }

    def validate(df: DataFrame) = {
      df
        .withColumn("sex_validated",
          when($"sex" === "F" or $"sex" === "M", true)
            .otherwise(false)
            .cast("boolean"))
        .withColumn("id_validated",
          when($"id" > 3, true)
            .otherwise(false)
            .cast("boolean"))
        .withColumn("all_validated",
          when($"sex_validated" === true and $"id_validated" === true, true)
            .otherwise(false)
            .cast("boolean"))
    }

    private def createHeaderDf(df: DataFrame) = {
      val header = df.schema.fields.map(_.name)
      import scala.collection.JavaConverters._
      ss.createDataFrame(List(Row.fromSeq(header.toSeq)).asJava, df.schema)
    }

  }
}
