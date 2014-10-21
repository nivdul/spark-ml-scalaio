package sparkapps

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


object Wordcount {

  def sparkJob() = {

    val file = "wordcount.txt"

    val conf = new SparkConf()
                  .setAppName("Spark word count")
                  .setMaster("local")

    val sc = new SparkContext(conf)

    val data = sc.textFile(file)

    val wordCounts = data.flatMap(line => line.split("\\s+"))
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)

    wordCounts.cache()

    println(wordCounts.collect().toList)

    val filteredCount = wordCounts.filter{
      case (key, value) => value > 2
    }

    println(filteredCount.collect().toList)
  }

  def main(args: Array[String])= sparkJob()

}
