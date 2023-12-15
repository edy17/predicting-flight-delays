package org.diehl.workflow

import org.apache.spark.sql.SparkSession
import org.diehl.conf.Conf

object DestinationAirportsProcessing {

  def main(args: Array[String]): Unit = {

    val orgDelayTableName = args(0)
    val destVectorizedWeatherTableName = args(1)
    val orgDestDelayTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession

    val orgWeatherPerFlightDf = sparkSession.read.format("delta").load(orgDelayTableName)
    val destWeather = sparkSession.read.format("delta").load(destVectorizedWeatherTableName)

    val weatherPerFlightDf = orgWeatherPerFlightDf.join(destWeather,
        orgWeatherPerFlightDf("DEST_WBAN") === destWeather("DEST_WBAN_WEATHER") &&
          orgWeatherPerFlightDf("ArrivalDate") === destWeather("DEST_DATE_TIME"),
        "inner")
      .drop("ArrivalDate", "DEST_WBAN", "DEST_DATE_TIME", "DEST_WBAN_WEATHER")
      .na.drop()
      .withColumnRenamed("Delay", "label")

    createTableIfNotExists(orgDestDelayTableName, sparkSession)
    weatherPerFlightDf.write.format("delta").mode("append").saveAsTable(s"delta.`$orgDestDelayTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$orgDestDelayTableName`").show()
    sparkSession.sql(s"SELECT * FROM delta.`$orgDestDelayTableName` LIMIT 5").show()
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          label int,
          originFeatures array<float>,
          destinationFeatures array<float>
        ) USING DELTA
      """)
  }
}