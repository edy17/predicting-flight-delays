package org.diehl.conf

import org.apache.log4j.Level
import org.apache.log4j.LogManager
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import java.time.ZoneId
import java.time.ZoneOffset

class Conf {

  def getSparkSession: SparkSession = {
    val conf = new SparkConf(true)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    SparkSession
      .builder()
      .config(conf)
      .getOrCreate()
  }

  def getAirportPerWeatherStationDf(filepath: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val airportWeatherStationRDD = spark.sparkContext.textFile(filepath)
      .map(x => x.split(",", -1))
      .mapPartitionsWithIndex((idx, iter) => if (idx == 0) iter.drop(1) else iter)
      .map(x => (x(0), (x(1), ZoneId.ofOffset("UTC", ZoneOffset.ofHours(x(2).toInt)).toString)))
    airportWeatherStationRDD.map({
      case (x, (y, z)) => (x, y, z)
    }).toDF("AirportID", "WBAN", "TimeZone")
  }

  def getSeed: Long = 42

  def getLogger: Logger = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    log
  }
}
