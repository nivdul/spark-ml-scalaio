package sparkapps

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD


object SparkApp {

  def sparkJob() = {

    val file = "/etc/passwd" // Specify the path to your data file
    val conf = new SparkConf()
          .setAppName("Spark BluePrint")
                 //Add more config if needed
          .setMaster("local")

    val sc = new SparkContext(conf)

      val data = sc.textFile(file, 2).cache()

      val numAs =
        data.filter(line => line.contains("a")).count()

      val numBs =
        data.filter(line => line.contains("b")).count()

      val piped = data.pipe("grep a").collect();

      println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }

 
  def main(args: Array[String])= sparkJob() 

}
