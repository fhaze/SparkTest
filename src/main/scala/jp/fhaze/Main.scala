package jp.fhaze

import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
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

  val validatedValues = Helper.validate(example)
  val onlyGoodValues  = validatedValues.filter($"status" === "O").drop($"status")
  val onlyBadValues   = validatedValues.filter($"status" === "X").drop($"status")

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
      parse(df).withColumn("status",
        when($"sex" === "F" or $"sex" === "M", lit("O"))
          .otherwise(lit("X")))
    }

    private def createHeaderDf(df: DataFrame) = {
      val header = df.schema.fields.map(_.name)
      import scala.collection.JavaConverters._
      ss.createDataFrame(List(Row.fromSeq(header.toSeq)).asJava, df.schema)
    }

    private def parse(df: DataFrame): Dataset[Row] = {
      df.select(df.columns.map(c => df.col(c).cast("string")): _*)
    }
  }
}
