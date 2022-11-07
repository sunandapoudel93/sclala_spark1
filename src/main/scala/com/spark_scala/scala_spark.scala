package com.spark_scala
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object scala_spark {
    def main(args : Array[String]): Unit = {
      Logger.getRootLogger.setLevel(Level.INFO)
      val sc = new SparkContext("local[*]" , "scala_spark")
      val lines = sc.textFile("C:\\Users\\Consultant\\Desktop\\scala_spark.txt")
      val words = lines.flatMap(line => line.split(' '))
      val wordsKVRdd = words.map(x => (x,1))
      val count = wordsKVRdd.reduceByKey((x,y) => x + y).map(x => (x._2,x._1)).sortByKey(false).map(x => (x._2, x._1)).take(10)
      count.foreach(println)
    }
}
