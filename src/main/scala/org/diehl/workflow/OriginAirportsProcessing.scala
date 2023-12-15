package org.diehl.workflow

import org.apache.spark.sql.SparkSession
import org.diehl.conf.Conf

object OriginAirportsProcessing {

  def main(args: Array[String]): Unit = {

    val flightsTableName = args(0)
    val orgVectorizedWeatherTableName = args(1)
    val orgDelayTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession

    val flightsDf = sparkSession.read.format("delta").load(flightsTableName)
    val originWeather = sparkSession.read.format("delta").load(orgVectorizedWeatherTableName)

    val orgWeatherPerFlightDf = flightsDf.join(originWeather,
        (flightsDf("ORIGIN_WBAN") === originWeather("ORIGIN_WBAN_WEATHER")) &&
          (flightsDf("FlightDate") === originWeather("ORIGIN_DATE_TIME")), "inner")
      .drop("FlightDate", "ORIGIN_WBAN", "ORIGIN_DATE_TIME", "ORIGIN_WBAN_WEATHER")
      .na.drop()

    createTableIfNotExists(orgDelayTableName, sparkSession)
    orgWeatherPerFlightDf.write.format("delta").mode("append").saveAsTable(s"delta.`$orgDelayTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$orgDelayTableName`").show()
    sparkSession.sql(s"SELECT * FROM delta.`$orgDelayTableName` LIMIT 5").show()
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          ArrivalDate timestamp,
          DEST_WBAN string,
          Delay int,
          originFeatures array<float>
        ) USING DELTA
      """)
  }
}