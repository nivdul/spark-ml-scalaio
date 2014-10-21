package sparkapps

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * Created by ludwineprobst on 20/10/2014.
 */
object Kmeans {

  def sparkJob() = {

    val conf = new SparkConf()
      .setAppName("Spark word count")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile("movies.csv")
    val parsedData = data.map(s => Vectors.dense(s.split("\\s+").map(_.toDouble)))

    // Cluster the data into two classes using KMeans
    val clusters = KMeans.train(parsedData, k = 4, maxIterations = 20)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

  }

  def main(args: Array[String])= sparkJob()

}
