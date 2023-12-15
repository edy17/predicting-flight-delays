package org.diehl.workflow

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.SparkSession
import org.diehl.conf.Conf

object VectorAssembling {

  def main(args: Array[String]): Unit = {

    val orgDestDelayTableName = args(0)
    val delayTableName = args(1)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    import sparkSession.implicits._

    val weatherPerFlightDf = sparkSession.read.format("delta").load(orgDestDelayTableName)

    val finalDf = weatherPerFlightDf.withColumn("originFeatures", array_to_vector($"originFeatures"))
      .withColumn("destinationFeatures", array_to_vector($"destinationFeatures"))

    val assemblerData = new VectorAssembler()
      .setInputCols(finalDf.columns.filter(x => x != "label"))
      .setOutputCol("features")
    val data = assemblerData.transform(finalDf)
      .drop(finalDf.columns.filter(x => x != "label"): _*)
      .withColumn("features", vector_to_array($"features", "float32"))

    createTableIfNotExists(delayTableName, sparkSession)
    data.write.format("delta").mode("append").saveAsTable(s"delta.`$delayTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$delayTableName`").show()
    sparkSession.sql(s"SELECT * FROM delta.`$delayTableName` LIMIT 5").show()
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          label int,
          features array<float>
        ) USING DELTA
      """)
  }
}