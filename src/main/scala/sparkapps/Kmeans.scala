package sparkapps

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Kmeans {

  def sparkJob() = {

    val conf = new SparkConf()
      .setAppName("Spark clustering with Kmeans")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the data and create RDD
    val data = sc.textFile("movies.csv")
    val parsedData = data.map(s => Vectors.dense(s.split("\\s+").map(_.toDouble)))

    // Cluster the data into 4 classes using KMeans
    val clusters = KMeans.train(parsedData, k = 4, maxIterations = 20)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    sc.stop()

  }

  def main(args: Array[String])= sparkJob()

}
