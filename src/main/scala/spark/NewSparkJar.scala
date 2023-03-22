package spark

import org.apache.spark.{SparkConf, SparkContext}

object NewSparkJar {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("someName").setAppName("WordCount").setMaster("local")
    //create spark context object
    val sc = new SparkContext(conf)

    //Create RDD from parallelize
    val dataSeq = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))
    val rdd = sc.parallelize(dataSeq)
    println(rdd.collect().foreach(print))
  }
}

// spark-submit --master local --class spark.NewSparkJar scala_jar_sergiu-1.0-SNAPSHOT-jar-with-dependencies.jar