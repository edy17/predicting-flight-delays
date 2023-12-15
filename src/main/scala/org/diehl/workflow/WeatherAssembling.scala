package org.diehl.workflow

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.asc
import org.apache.spark.sql.functions.lag
import org.diehl.conf.Conf

object WeatherAssembling {

  def main(args: Array[String]): Unit = {

    val interpolatedWeatherTableName = args(0)
    val orgVectorizedWeatherTableName = args(1)
    val destVectorizedWeatherTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    import sparkSession.implicits._

    val interpolatedWeatherDf = sparkSession.read.format("delta").load(interpolatedWeatherTableName)

    val assembler = new VectorAssembler()
      .setInputCols(interpolatedWeatherDf.columns.filter(x => (x != "DATE_TIME") && (x != "NAME") &&
        (x != "WBAN_WEATHER") && (x != "STATION_COORDINATES")))
      .setOutputCol("features")

    val assembledWeatherDf = assembler.transform(interpolatedWeatherDf)
      .drop(interpolatedWeatherDf.columns.filter(x => (x != "DATE_TIME") && (x != "WBAN_WEATHER") && (x != "features")): _*)

    val originWindowSpec = Window.partitionBy("ORIGIN_WBAN_WEATHER").orderBy(asc("ORIGIN_DATE_TIME"))
    val originWeather = tagColumns("ORIGIN_", assembledWeatherDf)
      .withColumn("ORIGIN_features_Lag_1", lag("ORIGIN_features", 1).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_2", lag("ORIGIN_features", 2).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_3", lag("ORIGIN_features", 3).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_4", lag("ORIGIN_features", 4).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_5", lag("ORIGIN_features", 5).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_6", lag("ORIGIN_features", 6).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_7", lag("ORIGIN_features", 7).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_8", lag("ORIGIN_features", 8).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_9", lag("ORIGIN_features", 9).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_10", lag("ORIGIN_features", 10).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_11", lag("ORIGIN_features", 11).over(originWindowSpec))
      .withColumn("ORIGIN_features_Lag_12", lag("ORIGIN_features", 12).over(originWindowSpec))
      .na.drop()
    val orgAssembler = new VectorAssembler()
      .setInputCols(originWeather.columns.filter(x => (x != "ORIGIN_DATE_TIME") && (x != "ORIGIN_WBAN_WEATHER")))
      .setOutputCol("originFeatures")
    val finalOriginWeather = orgAssembler.transform(originWeather)
      .drop(originWeather.columns.filter(x => (x != "ORIGIN_DATE_TIME") && (x != "ORIGIN_WBAN_WEATHER") && (x != "originFeatures")): _*)
      .withColumn("originFeatures", vector_to_array($"originFeatures", "float32"))

    val destWindowSpec = Window.partitionBy("DEST_WBAN_WEATHER").orderBy(asc("DEST_DATE_TIME"))
    val destWeather = tagColumns("DEST_", assembledWeatherDf)
      .withColumn("DEST_features_Lag_1", lag("DEST_features", 1).over(destWindowSpec))
      .withColumn("DEST_features_Lag_2", lag("DEST_features", 2).over(destWindowSpec))
      .withColumn("DEST_features_Lag_3", lag("DEST_features", 3).over(destWindowSpec))
      .withColumn("DEST_features_Lag_4", lag("DEST_features", 4).over(destWindowSpec))
      .withColumn("DEST_features_Lag_5", lag("DEST_features", 5).over(destWindowSpec))
      .withColumn("DEST_features_Lag_6", lag("DEST_features", 6).over(destWindowSpec))
      .withColumn("DEST_features_Lag_7", lag("DEST_features", 7).over(destWindowSpec))
      .withColumn("DEST_features_Lag_8", lag("DEST_features", 8).over(destWindowSpec))
      .withColumn("DEST_features_Lag_9", lag("DEST_features", 9).over(destWindowSpec))
      .withColumn("DEST_features_Lag_10", lag("DEST_features", 10).over(destWindowSpec))
      .withColumn("DEST_features_Lag_11", lag("DEST_features", 11).over(destWindowSpec))
      .withColumn("DEST_features_Lag_12", lag("DEST_features", 12).over(destWindowSpec))
      .na.drop()
    val destAssembler = new VectorAssembler()
      .setInputCols(destWeather.columns.filter(x => (x != "DEST_DATE_TIME") && (x != "DEST_WBAN_WEATHER")))
      .setOutputCol("destinationFeatures")
    val finalDestWeather = destAssembler.transform(destWeather)
      .drop(destWeather.columns.filter(x => (x != "DEST_DATE_TIME") && (x != "DEST_WBAN_WEATHER") && (x != "destinationFeatures")): _*)
      .withColumn("destinationFeatures", vector_to_array($"destinationFeatures", "float32"))

    createOriginTableIfNotExists(orgVectorizedWeatherTableName, sparkSession)
    finalOriginWeather.write.format("delta").mode("append").saveAsTable(s"delta.`$orgVectorizedWeatherTableName`")

    createDestTableIfNotExists(destVectorizedWeatherTableName, sparkSession)
    finalDestWeather.write.format("delta").mode("append").saveAsTable(s"delta.`$destVectorizedWeatherTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$orgVectorizedWeatherTableName`").show()
    sparkSession.sql(s"SELECT * FROM delta.`$orgVectorizedWeatherTableName` LIMIT 5").show()
  }

  private def createOriginTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          ORIGIN_DATE_TIME timestamp,
          ORIGIN_WBAN_WEATHER string,
          originFeatures array<float>
        ) USING DELTA
      """)
  }

  private def createDestTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          DEST_DATE_TIME timestamp,
          DEST_WBAN_WEATHER string,
          destinationFeatures array<float>
        ) USING DELTA
      """)
  }

  private def tagColumns(prefix: String, inputDf: DataFrame) = {
    val renamedColumns = inputDf.columns.map(c => inputDf(c).as(s"$prefix$c"))
    inputDf.select(renamedColumns: _*)
  }
}