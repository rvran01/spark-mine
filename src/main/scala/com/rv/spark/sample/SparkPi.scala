package com.rv.spark.sample


import scala.math.random
import org.apache.spark._


/** Computes an approximation to pi */
object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.executor.memory", "1g")
      .set("SPARK_LOCAL_IP", "localhost")
      .setMaster("local")
      .setAppName("Spark Pi")
    
    
    val spark = new SparkContext(conf)
    System.clearProperty("spark.master.port")
    
    val slices = if (args.length > 0) args(0).toInt else 4
    val n = 1000000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}