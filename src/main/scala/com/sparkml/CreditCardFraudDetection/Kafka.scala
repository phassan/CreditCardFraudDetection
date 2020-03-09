package com.sparkml.CreditCardFraudDetection

import com.sparkml.CreditCardFraudDetection.Driver.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.tuning.CrossValidatorModel

object Kafka {
  var model: CrossValidatorModel = _

  def ccfd(m: String): Unit = {

    if (m == "l") {
      model = CrossValidatorModel.load("linear-regression-model")
    } else if (m == "r") {
      model = CrossValidatorModel.load("random-forest-model")
    } else {
      println("Select appropriate model linear-regression(l) / random-forest(r)")
      return
    }

    import spark.implicits._

    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", false)
      .option("maxOffsetsPerTrigger", 1000)
      .option("subscribe", "ccfd")
      .load
      .selectExpr("CAST(value AS STRING)").as[(String)]

    var df = lines.withColumn("value", split($"value", ",")).select(
      $"value".getItem(0).cast("integer").as("Time"),
      $"value".getItem(1).cast("double").as("V1"),
      $"value".getItem(2).cast("double").as("V2"),
      $"value".getItem(3).cast("double").as("V3"),
      $"value".getItem(4).cast("double").as("V4"),
      $"value".getItem(5).cast("double").as("V5"),
      $"value".getItem(6).cast("double").as("V6"),
      $"value".getItem(7).cast("double").as("V7"),
      $"value".getItem(8).cast("double").as("V8"),
      $"value".getItem(9).cast("double").as("V9"),
      $"value".getItem(10).cast("double").as("V10"),
      $"value".getItem(11).cast("double").as("V11"),
      $"value".getItem(12).cast("double").as("V12"),
      $"value".getItem(13).cast("double").as("V13"),
      $"value".getItem(14).cast("double").as("V14"),
      $"value".getItem(15).cast("double").as("V15"),
      $"value".getItem(16).cast("double").as("V16"),
      $"value".getItem(17).cast("double").as("V17"),
      $"value".getItem(18).cast("double").as("V18"),
      $"value".getItem(19).cast("double").as("V19"),
      $"value".getItem(20).cast("double").as("V20"),
      $"value".getItem(21).cast("double").as("V21"),
      $"value".getItem(22).cast("double").as("V22"),
      $"value".getItem(23).cast("double").as("V23"),
      $"value".getItem(24).cast("double").as("V24"),
      $"value".getItem(25).cast("double").as("V25"),
      $"value".getItem(26).cast("double").as("V26"),
      $"value".getItem(27).cast("double").as("V27"),
      $"value".getItem(28).cast("double").as("V28"),
      $"value".getItem(29).cast("integer").as("Amount"),
      $"value".getItem(30).cast("integer").as("Class")).drop("value").drop("Class")

    val cols = df.columns ++ Array("prediction")

    val query = model.transform(df).select(cols.map(c => col(c)): _*).writeStream
      //      .outputMode("update")
      //      .format("console")
      //      .start()
      .queryName("writing_to_es")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "/tmp/")
      .option("es.resource", "transaction/ccfd")
      .option("es.nodes", "localhost")
      .start()

    query.awaitTermination()

  }
}