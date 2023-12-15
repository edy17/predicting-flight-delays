package org.diehl.workflow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.date_trunc
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.diehl.conf.Conf

object FlightsPreprocessing {

  def main(args: Array[String]): Unit = {

    val airportPerWeatherStationFilePath = args(0)
    val flightsFilepath = args(1)
    val flightsTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    import sparkSession.implicits._

    val airportWeatherStationDf = conf.getAirportPerWeatherStationDf(airportPerWeatherStationFilePath, sparkSession)

    // Normal flights or delayed by significant weather conditions
    val isNormalOrWeatherDelayFlight = (
      // Flights are delayed due to weather or flights are not late for any reason
      (($"WeatherDelay".isNotNull && $"WeatherDelay" > 0) ||
        (
          ($"CarrierDelay".isNull || $"CarrierDelay".cast("int") === 0) &&
            ($"NASDelay".isNull || $"NASDelay".cast("int") === 0) &&
            ($"SecurityDelay".isNull || $"SecurityDelay".cast("int") === 0) &&
            ($"LateAircraftDelay".isNull || $"LateAircraftDelay".cast("int") === 0) &&
            ($"ArrDelayMinutes".isNull || $"ArrDelayMinutes".cast("int") === 0)
          )
        )
        &&
        // Flights are not cancelled
        ($"Cancelled".isNull || $"Cancelled".cast("int") === 0)
      )

    val windowSpec = Window.partitionBy("FlightDate", "ArrivalDate", "ORIGIN_WBAN", "DEST_WBAN").orderBy($"WeatherDelay".desc)

    val flightsDf = sparkSession.read.option("header", "true")
      .csv(flightsFilepath)
      .withColumn("WeatherDelay", $"WeatherDelay".cast("int"))
      .where(isNormalOrWeatherDelayFlight)
      .as("df_a")
      .join(airportWeatherStationDf, $"df_a.OriginAirportID" === airportWeatherStationDf("AirportID"), "right")
      .withColumnRenamed("WBAN", "ORIGIN_WBAN")
      .withColumnRenamed("TimeZone", "OriginTimeZone")
      .drop("AirportID")
      .as("df_b")
      .join(airportWeatherStationDf, $"df_b.DestAirportID" === airportWeatherStationDf("AirportID"), "right")
      .withColumnRenamed("WBAN", "DEST_WBAN")
      .where($"ORIGIN_WBAN".isNotNull)
      .withColumn("ArrDelayMinutes", when($"ArrDelayMinutes".isNull, 0).otherwise($"ArrDelayMinutes"))
      .withColumn("FlightDate", concat($"FlightDate", lit(","), $"CRSDepTime"))
      .where($"FlightDate".isNotNull && $"CRSElapsedTime".isNotNull)
      .withColumn("FlightDate", date_trunc("Hour", to_utc_timestamp(to_timestamp($"FlightDate", "yyyy-MM-dd,HHmm"), $"OriginTimeZone")))
      .withColumn("ArrivalDate", date_trunc("Hour", timestamp_seconds(unix_timestamp($"FlightDate") + ($"CRSElapsedTime" * 60) + ($"ArrDelayMinutes" * 60))))
      .withColumn("Delay", when($"ArrDelayMinutes" > lit(15), 1).otherwise(0))
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select("FlightDate", "ArrivalDate", "ORIGIN_WBAN", "DEST_WBAN", "Delay")
      .dropDuplicates()

    createTableIfNotExists(flightsTableName, sparkSession)
    flightsDf.write.format("delta").mode("append").saveAsTable(s"delta.`$flightsTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$flightsTableName`").show(false)
    sparkSession.sql(s"SELECT * FROM delta.`$flightsTableName` LIMIT 5").show(false)
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          FlightDate timestamp,
          ArrivalDate timestamp,
          ORIGIN_WBAN string,
          DEST_WBAN string,
          Delay int
        ) USING DELTA
      """)
  }
}
