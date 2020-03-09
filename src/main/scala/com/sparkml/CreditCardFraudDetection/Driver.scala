package com.sparkml.CreditCardFraudDetection

import org.apache.spark.sql._
import org.apache.log4j._

object Driver {

  val spark = SparkSession.builder()
    .appName("SparkML-CCFD")
    .master("local[*]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    var parm1 = args(0).toLowerCase()
    var parm2 = args(1).toLowerCase()

    if (parm1 == "m") {
      Classification.getModel(parm2)
    } else if (parm1 == "k") {
      Kafka.ccfd(parm2)
    } else {
      println(s"""Please select correct option.
        First parameter:
                        m for generating classification model
                        k for fraud detection and saving the result to Elasticsearch
        Second parameter:
                        l for Logistic regression
                        r for Random forest""")
    }

    spark.stop()
  }

}
