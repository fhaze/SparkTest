package jp.fhaze

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  if (System.getProperty("os.name").startsWith("Windows")) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
  }

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


  val parsedValues = Helper.parse(example)
  val validatedValues = Helper.validate(parsedValues)

  val onlyGoodValues  = validatedValues.filter($"status_geral" === "O")
  val onlyBadValues   = validatedValues.filter($"status_geral" === "X")

  Helper.saveDataFrameToFileSystem(onlyGoodValues, outputFile)
  Helper.saveDataFrameToFileSystem(onlyBadValues, errorFile)

  object Helper extends Serializable {

    def saveDataFrameToFileSystem(df: DataFrame, destination: String) = {
      createHeaderDf(df).union(df).write
        .option("header", "false")
        .option("delimiter", "|")
        .csv(tempFile)

      val src = new Path(tempFile)
      val dst = new Path(destination)

      FileUtil.copyMerge(fs, src, fs, dst,true, sc.hadoopConfiguration,null)
    }

    def validate(df: DataFrame) = {
      df.withColumn("erro_sex",
        when(valiateSex($"sex"), lit("O"))
          .otherwise(lit("X")))
        .withColumn("erro_id",
          when($"id" > "3", lit("O"))
        .otherwise(lit("X")))
        .withColumn("status_geral", when($"erro_sex" === "O" and $"erro_id" === "O", lit("O")).otherwise(lit("X")))
    }

    def valiateSex(column: Column): Column = {
      column === "J"
    }

    private def createHeaderDf(df: DataFrame) = {
      val header = df.schema.fields.map(_.name)
      import scala.collection.JavaConverters._
      ss.createDataFrame(List(Row.fromSeq(header.toSeq)).asJava, df.schema)
    }

    def parse(df: DataFrame): Dataset[Row] = {
      df.select(df.columns.map(c => df.col(c).cast("string")): _*)
    }
  }
}
