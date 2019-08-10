package jp.fhaze

import java.util.UUID

import jp.fhaze.validator.{DummyValidator, ValidatorFactory}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
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
  val onlyGoodValues  = validatedValues.filter($"sex_validated" === true and $"id_validated" === true)
  val onlyBadValues   = validatedValues.filter($"sex_validated" === false and $"id_validated" === false)

  Helper.saveDataFrameToFileSystem(onlyGoodValues.drop("sex_validated", "id_validated"), outputFile)
  Helper.saveDataFrameToFileSystem(onlyBadValues.drop("sex_validated", "id_validated"), errorFile)

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
