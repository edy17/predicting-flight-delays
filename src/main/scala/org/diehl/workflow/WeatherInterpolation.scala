package org.diehl.workflow

import org.apache.commons.math3.analysis.interpolation.SplineInterpolator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.collect_list
import org.apache.spark.sql.functions.timestamp_seconds
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.diehl.conf.Conf

import java.util.Arrays.binarySearch
import scala.collection.JavaConverters._

object WeatherInterpolation {

  def main(args: Array[String]): Unit = {

    val weatherTableName = args(0)
    val interpolatedWeatherTableName = args(1)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession

    val weatherDf = sparkSession.read.format("delta").load(weatherTableName)

    val interpolatedWeatherDf = processStationWeather(weatherDf, sparkSession)
    createTableIfNotExists(interpolatedWeatherTableName, sparkSession)
    interpolatedWeatherDf.write.format("delta").mode("append").saveAsTable(s"delta.`$interpolatedWeatherTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$interpolatedWeatherTableName`").show(false)
    sparkSession.sql(s"SELECT * FROM delta.`$interpolatedWeatherTableName` LIMIT 5").show(false)
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

  private def processStationWeather(inputDf: DataFrame, spark: SparkSession): DataFrame = {
    val weatherColumns = Array("HourlyDewPointTemperature",
      "HourlyDryBulbTemperature",
      "HourlyWetBulbTemperature",
      "HourlyPrecipitation",
      "HourlyRelativeHumidity",
      "HourlyWindSpeed",
      "HourlyWindDirection",
      "HourlyStationPressure",
      "HourlyVisibility")
    var interpolatedWeatherDf = inputDf.na.fill(0, Array("HourlyPrecipitation"))
    weatherColumns.foreach(x => {
      if (x != "HourlyPrecipitation")
        interpolatedWeatherDf = splineInterpolation(interpolatedWeatherDf, x, spark)
    })
    interpolatedWeatherDf
  }

  private def splineInterpolation(inputDf: DataFrame, valueColName: String, spark: SparkSession): DataFrame = {
    import spark.implicits._

    val toInterpolate = inputDf.select($"WBAN_WEATHER",
        unix_timestamp($"DATE_TIME").cast("double").as("DATE_TIME"),
        col(valueColName))
      .orderBy("WBAN_WEATHER", "DATE_TIME")

    val definedPointsRdd = toInterpolate.where(col(valueColName).isNotNull)
      .groupBy("WBAN_WEATHER")
      .agg(collect_list("DATE_TIME"),
        collect_list(valueColName))
      .rdd.map(row => (row.getString(0), row.getList[Double](1), row.getList[Double](2)))
      .map({ case (id, x, y) =>
        val spline = new SplineInterpolator().interpolate(x.asScala.toArray, y.asScala.toArray)
        val polynomials = spline.getPolynomials
        val knots = spline.getKnots
        val n = spline.getN
        (id, (polynomials, knots, n))
      })

    val nonDefinedPointsRdd = toInterpolate.where(col(valueColName).isNull)
      .groupBy("WBAN_WEATHER")
      .agg(collect_list("DATE_TIME"))
      .rdd.map(row => (row.getString(0), row.getList[Double](1)))

    val interpolatedPointsDf = definedPointsRdd.join(nonDefinedPointsRdd)
      .flatMap({
        case (id, ((polynomials, knots, n), values)) =>
          values.asScala.toArray.map(v => {
            if (v < knots(0) || v > knots(n)) {
              throw new RuntimeException(s" Value $v is out of range for station $id")
            }
            var i = binarySearch(knots, v)
            if (i < 0) {
              i = -i - 2
            }
            if (i >= polynomials.length) {
              i = i - 1
            }
            val splineValue = polynomials(i).value(v - knots(i))
            (id, v, splineValue)
          })
      })
      .toDF("STATION_ID", "TIMESTAMP", "interpolated")
      .withColumn("TIMESTAMP", timestamp_seconds($"TIMESTAMP"))

    inputDf.join(interpolatedPointsDf,
        inputDf("WBAN_WEATHER") === interpolatedPointsDf("STATION_ID") && inputDf("DATE_TIME") === interpolatedPointsDf("TIMESTAMP"),
        "left")
      .withColumn(valueColName, when($"interpolated".isNull, col(valueColName))
        .otherwise($"interpolated"))
      .drop("STATION_ID", "TIMESTAMP", "interpolated")
  }
}