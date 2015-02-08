package com.rv.spark.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.rv.parser.Trigramme
import scala.math.Ordering
import org.apache.spark.rdd.RDD

case class Result(sortedDescKeyByOccurences: RDD[(String, Int)], elements : RDD[(String, Int)])

object SparkOccurenceCounter {
  
	def getKeyWithOccurences(ctx: SparkContext, filename : String) : Result= {
		val lines = ctx.textFile(filename, 3)
				
		val elements = lines.flatMap { line => Trigramme.decoupe(line, size=3).map(w => (w,1)) }
		println("number %s of elements in file %s".format(elements.toLocalIterator.size, filename))
		
		//group each elements by key
		val groupsByKey = elements.groupBy(f=> f._1)
		
		//compute occurence
		val keyWithOccurences = groupsByKey.toLocalIterator.toSeq.map(f=> {
			val key = f._1
					val indices = f._2.toList.map(e => e._2)
					val occurences = indices.foldLeft(0)(_+_)
					(key, occurences)
		}
				)
				
				val keyOccurencesRDD = ctx.parallelize(seq=keyWithOccurences, numSlices=5)
				keyOccurencesRDD.cache()
				//sort by occurences and print with simple scala -- humm very slow
				//keyWithOccurences.sortBy(f => f._2) (Ordering.Int).takeRight(10).foreach(f=> println("simple scala : ey(%s) - occurence(%s)".format(f._1,f._2)))
				val sortedDescKeyByOccurences = keyOccurencesRDD.sortBy(f=> f._2, ascending=false, numPartitions=5)
				Result(sortedDescKeyByOccurences, elements)
	}
  
  def compute(ctx: SparkContext, filename: String, showNumber: Int ) = {
    val res = getKeyWithOccurences(ctx, filename)
    res.sortedDescKeyByOccurences.take(num=10).foreach(f=> println("with rdd filename(%s): key(%s) - occurence(%s)".format(filename, f._1,f._2)))
    println("number (%s) of elements  - number of keys(%s) in file %s".format(res.elements.toLocalIterator.size, res.sortedDescKeyByOccurences.toLocalIterator.size, filename))
  }
  
  def main(args: Array[String]) {
 
  
  val sparkConf = new SparkConf()
      .set("spark.executor.memory", "2g")
      .set("SPARK_LOCAL_IP", "localhost")
      .setMaster("local")
      .setAppName("Spark BigFile")
      
  val ctx = new SparkContext(sparkConf)
  val showNumber = 10
  val filename4 = "/home/maya/data/fidmarques/d_export_4.txt"
  compute(ctx, filename4, showNumber)
  
  val filename16 = "/home/maya/data/fidmarques/d_export_16.txt"
  compute(ctx, filename16, showNumber)
  
  ctx.stop()
  }
}