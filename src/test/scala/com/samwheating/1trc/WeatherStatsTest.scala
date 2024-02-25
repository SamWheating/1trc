package com.samwheating.trc

/** A simple test for everyone's favourite wordcount example.
  */

import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession

import com.holdenkarau.spark.testing.{
  SharedSparkContext,
  ScalaDataFrameSuiteBase
}

import org.apache.spark.sql._

case class InputRow(station: String, measure: Double)
case class OutputRow(station: String, max: Double, min: Double, mean: Double)

class WeatherStatsTest extends AnyFunSuite with ScalaDataFrameSuiteBase {
  test("Weather stats are computed as expected") {

    val session = SparkSession.builder().getOrCreate()

    import session.implicits._

    val sampleData = List(
      InputRow("Vancouver", 10.0),
      InputRow("Vancouver", 20.0),
      InputRow("Ottawa", -5.0),
      InputRow("Ottawa", 25.0)
    )

    val input = sc.parallelize(sampleData).toDF

    val actual = WeatherStats.ComputeWeatherStats(input)

    val expectedData = List(
      OutputRow("Ottawa", 25.0, -5, 10.0),
      OutputRow("Vancouver", 20.0, 10.0, 15.0)
    )

    val expected = sc.parallelize(expectedData).toDF

    assertDataFrameDataEquals(actual, expected)

  }
}
