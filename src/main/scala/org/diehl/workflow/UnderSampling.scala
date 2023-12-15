package org.diehl.workflow

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rand
import org.apache.spark.sql.functions.row_number
import org.diehl.conf.Conf

object UnderSampling {

  def main(args: Array[String]): Unit = {

    val delayTableName = args(0)
    val trainDelayTableName = args(1)
    val testDelayTableName = args(2)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    import sparkSession.implicits._

    val data = sparkSession.read.format("delta").load(delayTableName)

    //Random elimination of examples from the majority class
    val trueLabelCount = data.filter($"label" === 1).count
    val falseLabelCount = data.filter($"label" === 0).count
    val minorCount = if (trueLabelCount > falseLabelCount) falseLabelCount else trueLabelCount
    val windowLabel = Window.partitionBy("label").orderBy("random")
    val underSampledDf = data.withColumn("random", rand(seed = conf.getSeed))
      .withColumn("row", row_number().over(windowLabel))
      .filter($"row" <= minorCount)
      .withColumn("random", rand(seed = conf.getSeed))

    // Split the balanced data into training and test sets (20% held out for testing)
    var trainingData = underSampledDf.filter($"row" <= (minorCount * 0.8))
      .orderBy("random")
    val testData = underSampledDf.except(trainingData)
      .orderBy("random")
      .drop("row", "random")

    trainingData = trainingData.drop("row", "random")

    createTableIfNotExists(trainDelayTableName, sparkSession)
    createTableIfNotExists(testDelayTableName, sparkSession)
    trainingData.write.format("delta").mode("append").saveAsTable(s"delta.`$trainDelayTableName`")
    testData.write.format("delta").mode("append").saveAsTable(s"delta.`$testDelayTableName`")

    sparkSession.sql(s"SELECT COUNT(*) FROM delta.`$trainDelayTableName`").show(false)
    sparkSession.sql(s"SELECT * FROM delta.`$trainDelayTableName` LIMIT 5").show(false)
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