package com.samwheating.trc

/** Everyone's favourite wordcount example.
  */

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object WeatherStats {
  // Given a list of cities and temparatures,
  // compute the max, min, mean temparature for each city.
  def ComputeWeatherStats(rawData: DataFrame): DataFrame = {

    val statsDf = rawData
      .groupBy("station")
      .agg(
        max("measure").as("max"),
        min("measure").as("min"),
        avg("measure").as("mean")
      )
      .sort(asc("station"))

    statsDf
  }

}
