package org.diehl.workflow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.last
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.sequence
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.diehl.conf.Conf

object WeatherResampling {

  def main(args: Array[String]): Unit = {

    val rawWeatherTableName = args(0)
    val weatherTableName = args(1)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    import sparkSession.implicits._

    val rawWeatherDf = sparkSession.read.format("delta").load(rawWeatherTableName)
    val sampleRef = generateReSamplingRefDfByHour(rawWeatherDf, sparkSession)
    val windowSpecAgg = Window.partitionBy("WBAN_WEATHER")
    val weatherDf = reSampleDfByHour(rawWeatherDf, sampleRef)
      .withColumn("count", count($"DATE_TIME").over(windowSpecAgg))
      .filter($"count" > 3)
      .drop("count")

    createTableIfNotExists(weatherTableName, sparkSession)
    weatherDf.write.format("delta").mode("append").saveAsTable(s"delta.`$weatherTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$weatherTableName`").show(false)
    sparkSession.sql(s"SELECT * FROM delta.`$weatherTableName` LIMIT 5").show(false)
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          DATE_TIME timestamp,
          NAME string,
          WBAN_WEATHER string,
          STATION_COORDINATES string,
          HourlyDewPointTemperature double,
          HourlyDryBulbTemperature double,
          HourlyWetBulbTemperature double,
          HourlyPrecipitation double,
          HourlyRelativeHumidity double,
          HourlyWindSpeed double,
          HourlyWindDirection double,
          HourlyStationPressure double,
          HourlyVisibility double
        ) USING DELTA
      """)
  }

  private def reSampleDfByHour(rawDf: DataFrame, refDf: DataFrame): DataFrame = {
    val inputDf = rawDf.withColumnRenamed("DATE_TIME", "TIMESTAMP")
      .withColumnRenamed("WBAN_WEATHER", "STATION_ID")
      .withColumnRenamed("NAME", "STATION_NAME")
      .withColumnRenamed("STATION_COORDINATES", "STATION_COORD")

    refDf.join(inputDf,
        refDf("WBAN_WEATHER") === inputDf("STATION_ID") && refDf("DATE_TIME") === inputDf("TIMESTAMP"), "left")
      .drop("TIMESTAMP", "STATION_NAME", "STATION_ID", "STATION_COORD")
      .orderBy("WBAN_WEATHER", "DATE_TIME")
  }

  private def generateReSamplingRefDfByHour(rawDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val tsAreDefined = when($"HourlyDewPointTemperature".isNotNull &&
      $"HourlyDryBulbTemperature".isNotNull &&
      $"HourlyWetBulbTemperature".isNotNull &&
      $"HourlyRelativeHumidity".isNotNull &&
      $"HourlyWindSpeed".isNotNull &&
      $"HourlyWindDirection".isNotNull &&
      $"HourlyStationPressure".isNotNull &&
      $"HourlyVisibility".isNotNull, $"DATE_TIME").otherwise(null)

    val windowUnboundedValues = Window.partitionBy("WBAN_WEATHER").orderBy(asc("DATE_TIME"))
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val windowStationTemporalOrderAsc = Window.partitionBy("WBAN_WEATHER").orderBy(asc("DATE_TIME"))
    val windowStationTemporalOrderDesc = Window.partitionBy("WBAN_WEATHER").orderBy(desc("DATE_TIME"))

    val minEpochPerStation = rawDf.withColumn("DATE_TIME", unix_timestamp($"DATE_TIME"))
      .withColumn("firstNonNullDate", first(tsAreDefined, ignoreNulls = true).over(windowUnboundedValues))
      .filter($"DATE_TIME" === $"firstNonNullDate")
      .withColumn("rowTemporalOrder", row_number().over(windowStationTemporalOrderAsc))
      .filter($"rowTemporalOrder" === 1)
      .drop("rowTemporalOrder")
      .select("WBAN_WEATHER", "NAME", "STATION_COORDINATES", "firstNonNullDate")

    val maxEpochPerStation = rawDf.withColumn("DATE_TIME", unix_timestamp($"DATE_TIME"))
      .withColumn("lastNonNullDate", last(tsAreDefined, ignoreNulls = true).over(windowUnboundedValues))
      .filter($"DATE_TIME" === $"lastNonNullDate")
      .withColumn("rowTemporalOrder", row_number().over(windowStationTemporalOrderDesc))
      .filter($"rowTemporalOrder" === 1)
      .drop("rowTemporalOrder")
      .select("WBAN_WEATHER", "lastNonNullDate")

    val epochPerStation = minEpochPerStation.join(maxEpochPerStation, "WBAN_WEATHER", "inner")

    epochPerStation.withColumn("DATE_TIME",
        explode(sequence($"firstNonNullDate", $"lastNonNullDate", lit(3600)))) // step = 1 hour in seconds
      .withColumn("DATE_TIME", timestamp_seconds($"DATE_TIME"))
      .drop("firstNonNullDate", "lastNonNullDate")
  }
}