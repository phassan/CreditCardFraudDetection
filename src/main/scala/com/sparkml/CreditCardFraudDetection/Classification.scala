package com.sparkml.CreditCardFraudDetection

import com.sparkml.CreditCardFraudDetection.Driver.spark
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorSizeHint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.ml.param.ParamMap
import vegas.sparkExt._
import vegas._

object Classification {

  import spark.implicits._
  var paramGrid: Array[ParamMap] = _
  var pipeline: Pipeline = _

  def getModel(m: String): Unit = {
    var df = spark.read.format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("creditcard.csv")

    for (col <- df.columns) {
      val count = df.filter(df(col).isNull || df(col) === "" || df(col).isNaN).count()
      println(col + " has " + count + " null or empty values")
    }

    val classFreq = df.groupBy("Class").count()
    classFreq.show()

    Vegas("Class count").
      withDataFrame(classFreq).
      encodeX("Class", Nominal).
      encodeY("count", Quantitative).
      mark(Bar).
      show

    //    Check Correlation
    //        df.columns.combinations(2).foreach(c => {
    //          println(c(0) + " vs. " + c(1))
    //          df.select(corr(c(0), c(1))).show()
    //        })

    //    Class weighting for the unbalanced dataset      
    //        var frauds = df.filter($"Class" === 1)
    //        var nonFrauds = df.filter($"Class" === 0)
    //        val numFrauds = frauds.count()
    //        val datasetSize = df.count()
    //        val balancingRatio = (datasetSize - numFrauds).toDouble / datasetSize
    //        df = df.withColumn("classWeightCol", when($"Class" === 1, balancingRatio).otherwise(1.0 - balancingRatio))
    //        df.select("classWeightCol").distinct().show()

    //   Undersampling for the unbalanced dataset
    val fraud = df.filter($"Class" === 1)
    val nonFraud = df.filter($"Class" === 0)
    val ratio = fraud.count().toDouble / df.count().toDouble
    val nonFraudSample = nonFraud.sample(false, ratio)
    df = fraud.unionAll(nonFraudSample)

    val amountVA = new VectorAssembler()
      .setInputCols(Array("Amount"))
      .setOutputCol("Vector_Amount")
      .setHandleInvalid("skip")

    val scaler = new StandardScaler()
      .setInputCol("Vector_Amount")
      .setOutputCol("Scaled_Amount")

    val dropCols = Array("Time", "Amount", "Vector_Amount", "Class")
    val cols = df.columns.filter(c => !dropCols.contains(c)) ++ Array("Scaled_Amount")

    val vectorSizeHint = new VectorSizeHint()
      .setInputCol("Scaled_Amount")
      .setSize(1)

    val assembler = new VectorAssembler()
      .setInputCols(cols)
      .setOutputCol("features")
      .setHandleInvalid("skip")

    var Array(train, test) = df.randomSplit(Array(.8, .2), 42)

    if (m == "l") {
      val classifier = new LogisticRegression()
        //      .setWeightCol("classWeightCol")
        .setLabelCol("Class")
        .setFeaturesCol("features")
        .setMaxIter(10)

      paramGrid = new ParamGridBuilder()
        .addGrid(classifier.aggregationDepth, Array(2, 5, 10))
        .addGrid(classifier.elasticNetParam, Array(0.0, 0.5, 1.0))
        .addGrid(classifier.fitIntercept, Array(false, true))
        .addGrid(classifier.regParam, Array(0.01, 0.5, 2.0))
        .build()

      pipeline = new Pipeline().setStages(Array(amountVA, scaler, vectorSizeHint, assembler, classifier))

    } else if (m == "r") {
      val classifier = new RandomForestClassifier().setLabelCol("Class").setFeaturesCol("features")

      paramGrid = new ParamGridBuilder()
        //      .addGrid(classifier.maxBins, Array(25, 28, 31))
        .addGrid(classifier.maxDepth, Array(3, 4, 6, 8))
        .addGrid(classifier.numTrees, Array(5, 20, 50))
        .addGrid(classifier.impurity, Array("entropy", "gini"))
        .build()

      pipeline = new Pipeline().setStages(Array(amountVA, scaler, vectorSizeHint, assembler, classifier))

    } else {
      println("Select appropriate model linear-regression(l) / random-forest(r)")
      return
    }

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("Class")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
    //      .setMetricName("areaUnderPR")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(3)

    val model = cv.fit(train)
    val predictions = model.transform(test)
    //    predictions.show(false)

    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy of Model = ${accuracy}")

    val results = predictions.select("features", "Class", "prediction")
    val predictionAndLabels = results.
      select($"prediction", $"Class").
      as[(Double, Double)].
      rdd

    // Instantiate a new metrics objects
    val bMetrics = new BinaryClassificationMetrics(predictionAndLabels)
    val mMetrics = new MulticlassMetrics(predictionAndLabels)
    val labels = mMetrics.labels

    // Print out the Confusion matrix
    println("Confusion matrix:")
    println(mMetrics.confusionMatrix)

    // Precision by label
    labels.foreach { l =>
      println(s"Precision($l) = " + mMetrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + mMetrics.recall(l))
    }

    // False positive rate by label
    labels.foreach { l =>
      println(s"FPR($l) = " + mMetrics.falsePositiveRate(l))
    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + mMetrics.fMeasure(l))
    }

    if (m == "l") {
      model.write.overwrite().save("linear-regression-model")
    } else if (m == "r") {
      model.write.overwrite().save("random-forest-model")
    } else {
      println("Select appropriate model linear-regression(l) / random-forest(r)")
      return
    }

  }

}