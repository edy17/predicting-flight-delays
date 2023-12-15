package org.diehl.workflow

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.concat
import org.apache.spark.sql.functions.date_trunc
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.radians
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.functions.when
import org.diehl.conf.Conf

object WeatherPreprocessing {

  def main(args: Array[String]): Unit = {

    val airportPerWeatherStationFilePath = args(0)
    val weatherFilepath = args(1)
    val rawWeatherTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession

    val airportWeatherStationDf = conf.getAirportPerWeatherStationDf(airportPerWeatherStationFilePath, sparkSession)

    val rawWeatherDf = processStationWeather(weatherFilepath, airportWeatherStationDf, sparkSession)

    createTableIfNotExists(rawWeatherTableName, sparkSession)
    rawWeatherDf.write.format("delta").mode("append").saveAsTable(s"delta.`$rawWeatherTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$rawWeatherTableName`").show(false)
    sparkSession.sql(s"SELECT * FROM delta.`$rawWeatherTableName` LIMIT 5").show(false)
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

  private def processStationWeather(filePath: String, tzDf: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val weatherColumns = Array("HourlyDewPointTemperature",
      "HourlyDryBulbTemperature",
      "HourlyWetBulbTemperature",
      "HourlyPrecipitation",
      "HourlyRelativeHumidity",
      "HourlyWindSpeed",
      "HourlyWindDirection",
      "HourlyStationPressure",
      "HourlyVisibility")

    val windowSpec = Window.partitionBy("WBAN_WEATHER", "DATE_TIME").orderBy("DATE")
    val windowSpecAgg = Window.partitionBy("WBAN_WEATHER", "DATE_TIME")

    val columnsMap = scala.collection.mutable.LinkedHashMap[String, Column]()
    weatherColumns.foreach(x => {
      val currentCol = if (x == "HourlyWindDirection") {
        radians(when(col(x) === "VRB", null)
          .otherwise(col(x)))
      } else if (x == "HourlyPrecipitation") {
        when(col(x).endsWith("s"), expr(s"substring(${x}, 1, length(${x})-1)"))
          .when(col(x) === "T", "0,004")
          .otherwise(col(x))
      } else {
        when(col(x).endsWith("s") || col(x).endsWith("V"), expr(s"substring(${x}, 1, length(${x})-1)"))
          .when(col(x) === "*", null)
          .otherwise(col(x))
      }
      columnsMap += (x -> avg(currentCol).over(windowSpecAgg))
    })

    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)
      .withColumn("WBAN_WEATHER", expr("substring(STATION, length(STATION) - 4, 5)"))
      .as("df_a")
      .join(tzDf, $"df_a.WBAN_WEATHER" === tzDf("WBAN"), "inner")
      .withColumn("DATE", to_utc_timestamp(to_timestamp($"DATE"), $"TimeZone"))
      .withColumn("DATE_TIME", date_trunc("Hour", $"DATE"))
      .withColumn("STATION_COORDINATES", concat($"LATITUDE", lit(", "), $"LONGITUDE"))
      .selectExpr(Array.concat(Array("NAME", "WBAN_WEATHER", "STATION_COORDINATES", "DATE", "DATE_TIME"), weatherColumns): _*)
      .orderBy("WBAN_WEATHER", "DATE_TIME")
      .withColumns(columnsMap.toMap)
      .withColumn("row", row_number.over(windowSpec))
      .where($"row" === 1)
      .drop("row", "DATE")
  }
}