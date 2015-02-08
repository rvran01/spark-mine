package com.rv.spark.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkBigFileCounter {
  
  def main(args: Array[String]) {
 
    val filename = "/home/maya/akka-mine-sample/frwiki-20140331-pages-articles-multistream-index.txt"
  
  val sparkConf = new SparkConf()
      .set("spark.executor.memory", "2g")
      .set("SPARK_LOCAL_IP", "localhost")
      .setMaster("local")
      .setAppName("Spark BigFile")
      
  val ctx = new SparkContext(sparkConf)
  val lines = ctx.textFile(filename, 1)
  //lines.cache
  
  val res = lines.flatMap(l=> l.split(" ")).map(w => (w,1)).count
  println("count %s".format(res))
  
    ctx.stop()
  }
}