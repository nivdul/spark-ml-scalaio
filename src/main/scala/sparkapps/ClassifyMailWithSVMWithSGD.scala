package sparkapps

import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

object ClassifyMailWithSVMWithSGD {

  def sparkJob() = {

    val conf = new SparkConf()
      .setAppName("Spark classify mail as spam or non-spam with SVM")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the data and create RDD
    val data = sc.textFile("spambase.csv")

    val parsedData = data.map { line =>
      val parts = line.split(',').map(_.toDouble)

      // Extract features for training as LabeledPoint
      // LabeledPoint is a couple (label, features)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    // Split data into 2 sets : training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4))
    val training = splits(0)
    val test = splits(1)

    // Training model on training set with 100 iterations
    val model = SVMWithSGD.train(training, 100)

    // Validation
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy " + accuracy)
    println("metrics " + metrics.areaUnderPR())
    println("metrics " + metrics.areaUnderROC())

    // stop context
    sc.stop()

  }

  def main(args: Array[String])= sparkJob()
}
