package com.rv.spark.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.rv.parser.Trigramme
import scala.math.Ordering
import org.apache.spark.rdd.RDD

case class KeyOccurences(key : String, occurences : Int, indice : Int)
case class Result(sortedDescKeyByOccurences: Map[String, KeyOccurences], elements : List[(String, Int)])
case class Reference(key: String, occurencesOpt : Option[Int], indiceOpt : Option[Long])

object SparkOccurenceCounter {
  
	def getKeysWithOccurences(ctx: SparkContext, filename : String) : Result= {
		val lines = ctx.textFile(filename, 3)
				
		val elements = lines.flatMap { line => Trigramme.decoupe(line, size=3).map(w => (w,1)) }
		println("number %s of elements in file %s".format(elements.toLocalIterator.size, filename))
		elements.cache()
    
		//group each elements by key
		val groupsByKey = elements.groupBy(f=> f._1)
		
		//compute occurence
		val keyWithOccurences = groupsByKey.toLocalIterator.toList.map(f=> {
			val key = f._1
			val indices = f._2.toList.map(e => e._2)
			val occurences = indices.foldLeft(0)(_+_)
			(key, occurences)
		  })
		  //sort by occurences desc
      .sortBy(f=> f._2)(Ordering.Int.reverse)
      //add index
      .zipWithIndex
      
		val sortedDescKeyByOccurences = keyWithOccurences.map(f=> f._1._1 -> KeyOccurences(f._1._1, f._1._2, f._2)).toMap
		 
		Result(sortedDescKeyByOccurences, elements.toLocalIterator.toList)
	}
  
  def compute(ctx: SparkContext, filename: String, showNumber: Int ) = {
    val res = getKeysWithOccurences(ctx, filename)
    res.sortedDescKeyByOccurences.values.take(10).foreach(f=> println("with rdd filename(%s): key(%s) - occurence(%s) - indice(%s)".format(filename, f.key, f.occurences,f.indice)))
    println("number (%s) of elements  - number of keys(%s) in file %s".format(res.elements.size, res.sortedDescKeyByOccurences.size, filename))
  }
  
  /**
   * looking to the Reference corresponding to the toCheck key
   */
  def getReference(references : Map[String, KeyOccurences], toCheck : String) : Reference = {
		//println(" *** check(%s)".format(toCheck))
    val filteredOpt = references.get(key = toCheck)
    filteredOpt match {
      case Some(elt) => {
        Reference(key = elt.key, occurencesOpt = Some(elt.occurences), indiceOpt = Some(elt.indice))
      }
      case None => Reference(key = toCheck, None, None)
    }
  }
  
  def main(args: Array[String]) {
 
    def printKeySearch(filename : String, keyToSearch : String, reference : Reference) = {
      println("key(%s) - occurences(%s) - indice(%s) in file(%s)".format(reference.key, reference.occurencesOpt.getOrElse("NOT_FOUND"), reference.indiceOpt.getOrElse("NOT_FOUND"), filename))
    }
    val sparkConf = new SparkConf()
      .set("spark.executor.memory", "2g")
      .set("SPARK_LOCAL_IP", "localhost")
      .setMaster("local")
      .setAppName("Spark BigFile")
      
    val ctx = new SparkContext(sparkConf)
    //val showNumber = 10
    val filename4 = "/home/maya/data/fidmarques/d_export_4.txt"
    //compute(ctx, filename4, showNumber)
    
    val result4RDD = ctx.parallelize(seq=Seq(getKeysWithOccurences(ctx, filename4)), numSlices=10).cache()
    val references = result4RDD.first().sortedDescKeyByOccurences
  
    /*val toCheck1 = "0G "
    printKeySearch(filename = filename4, keyToSearch = toCheck1, reference = getReference(sortedDescKeyByOccurences = keyOccurences, toCheck1))
    */
  
    //apply research on candidate file - take distinct elements
    val candidateLinesRDD = ctx.textFile(path="/home/maya/data/fidmarques/d_export_candidate.txt", minPartitions=3)
    val candidates = candidateLinesRDD.flatMap ( line => Trigramme.decoupe(line, size=3) ).distinct()
    //candidates.foreach { c => printKeySearch(filename = filename4, keyToSearch = c, referenceOpt = getReference(result = result4RDD.first(), c)) }
    def outputFormat(list : List[Reference], separator: String) : List[String]= {
	    list.map { l =>
        "key["+ l.key +"]" + separator + "occurences["+ l.occurencesOpt.getOrElse(-1) + "]" + separator + "indice[" + l.indiceOpt.getOrElse(-1) + "]"
      }
    }
    val results = outputFormat( candidates.toLocalIterator.toList.map (c => getReference(references, toCheck = c) ), ":")
    val resultsRDD = ctx.parallelize(seq=results.toSeq, numSlices = 1)
    resultsRDD.saveAsTextFile("/home/maya/data/fidmarques/resultFrom4.txt" )
 
  
    //val filename16 = "/home/maya/data/fidmarques/d_export_16.txt"
    //compute(ctx, filename16, showNumber)
  
    ctx.stop()
  }
}