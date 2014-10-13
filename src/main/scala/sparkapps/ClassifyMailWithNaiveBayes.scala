package sparkapps

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}

object ClassifyMailWithNaiveBayes {

  def sparkJob() = {
    // Specify the path to your data file
    val conf = new SparkConf()
        .setAppName("Spark classify mail as spam or non-spam with naive bayes")
        //Add more config if needed
        .setMaster("local")

    val sc = new SparkContext(conf)

    // load the data
    val data = sc.textFile("spambase.csv")

    val parsedData = data.map { line =>
      val parts = line.split(',').map(_.toDouble)

      // prepare data into RDD[LabeledPoint]
      // LabeledPoint is a couple (label, features)
      LabeledPoint(parts(0), Vectors.dense(parts.tail))
    }

    // Split data into 2 sets : training (60%) and test (40%).
    val splits = parsedData.randomSplit(Array(0.6, 0.4), seed = 11L)
    // training split
    val training = splits(0)
    // test split
    val test = splits(1)

    // training model on training set with the parameter lambda = 1
    // why 1 for lambda ?
    val model = NaiveBayes.train(training, lambda = 1.0)

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()

    println("accuracy " + accuracy)

  }

  def main(args: Array[String])= sparkJob()
}
