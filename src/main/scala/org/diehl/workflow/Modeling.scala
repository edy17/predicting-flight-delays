package org.diehl.workflow

import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.functions.array_to_vector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.diehl.conf.Conf

object Modeling {

  def main(args: Array[String]): Unit = {

    val trainDelayTableName = args(0)
    val testDelayTableName = args(1)
    val trainRocTableName = args(2)
    val testRocTableName = args(3)
    val modelsLocation = args(4)

    val conf = new Conf()
    val sparkSession = conf.getSparkSession
    val log = conf.getLogger
    import sparkSession.implicits._

    val trainData = sparkSession.read.format("delta").load(trainDelayTableName)
      .withColumn("features", array_to_vector($"features"))
    val testData = sparkSession.read.format("delta").load(testDelayTableName)
      .withColumn("features", array_to_vector($"features"))

    // Train a RandomForest model
    // To do : Hyperparameter tuning with Cross-Validation
    val randomForest = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(25)
      .setMaxDepth(13)

    // Fit model
    val model = randomForest.fit(trainData)
    model.save(modelsLocation + "/rf_model")
    val trainAreaUnderROC = model.summary.asBinary.areaUnderROC
    val trainRoc = model.summary.asBinary.roc
      .withColumn("areaUnderROC", lit(trainAreaUnderROC).cast("double"))

    // Make predictions
    val predictions = model.transform(testData).select($"prediction", $"label".cast("double"))
    val metrics = new BinaryClassificationMetrics(predictions.rdd.map(x => (x.getDouble(0), x.getDouble(1))))
    val testAreaUnderROC = metrics.areaUnderROC()
    val testRoc = metrics.roc.toDF("FPR", "TPR")
      .withColumn("areaUnderROC", lit(testAreaUnderROC).cast("double"))

    createTableIfNotExists(trainRocTableName, sparkSession)
    createTableIfNotExists(testRocTableName, sparkSession)
    trainRoc.write.format("delta").mode("append").saveAsTable(s"delta.`$trainRocTableName`")
    testRoc.write.format("delta").mode("append").saveAsTable(s"delta.`$testRocTableName`")

    log.warn(s"Train areaUnderROC : $trainAreaUnderROC")
    log.warn(s"Test areaUnderROC : $testAreaUnderROC")
  }

  private def createTableIfNotExists(tableName: String, spark: SparkSession): Unit = {
    spark.sql(
      s"""
        CREATE TABLE IF NOT EXISTS delta.`$tableName` (
          FPR double,
          TPR double,
          areaUnderROC double
        ) USING DELTA
      """)
  }
}
