package com.samwheating.trc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SaveMode

/** Use this to test the app locally, from sbt: sbt "run inputFile.txt
  * outputFile.txt" (+ select CountingLocalApp when prompted)
  */
object TrillionRowChallengeLocalApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("Trillion Row Challenge")

  Runner.run(conf, inputFile, outputFile)
}

/** Use this when submitting the app to a cluster with spark-submit
  */
object TrillionRowChallengeApp extends App {
  val (inputFile, outputFile) = (args(0), args(1))

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, outputFile)
}

object Runner {
  def run(conf: SparkConf, inputPath: String, outputPath: String): Unit = {
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()

    val inputDf  = spark.read.parquet(inputPath)
    val outputDf = WeatherStats.ComputeWeatherStats(inputDf)

    outputDf.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputPath)
  }
}
