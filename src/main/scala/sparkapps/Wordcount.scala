package sparkapps

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Wordcount {

  def sparkJob() = {

    val conf = new SparkConf()
                  .setAppName("Spark word count")
                  .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the data and create RDD
    val data = sc.textFile("wordcount.txt")

    val wordCounts = data.flatMap(line => line.split("\\s+")) // parse data
                         .map(word => (word, 1)) // map step
                         .reduceByKey(_ + _) // reduce step

    // Persist data
    wordCounts.cache()

    println(wordCounts.collect().toList)

    // Keep words which appear more than 2 times
    val filteredWordCount = wordCounts.filter{
      case (key, value) => value > 2
    }

    filteredWordCount.count()

    println(filteredWordCount.collect().toList)

    sc.stop()
  }

  def main(args: Array[String])= sparkJob()

}
