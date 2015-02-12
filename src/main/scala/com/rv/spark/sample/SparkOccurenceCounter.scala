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
  
  /**
   * compute the sum of occurences of trigramme for each candidate line
   * for each candidate line, call decoupe and for each trigramme some occurences
   */
  def sumOccurences(candidateLine : String, references : Map[String, KeyOccurences]) = {
    Trigramme.decoupe(candidateLine, size=3)
      .map { t => getReference(references, toCheck = t).occurencesOpt }
      .flatten.foldLeft(0)(_+_)
  }
  
  /**
   * apply research on candidate file - take distinct elements (trigramme)
   * compute each distinct trigramme of the file
   */
  def parseCandidatesTrigrammeWithReference(candidateLinesRDD : RDD[String], references : Map[String, KeyOccurences]) = {
    val candidates = candidateLinesRDD.flatMap ( line => Trigramme.decoupe(line, size=3) ).distinct()
    val results = outputFormat(candidates.toLocalIterator.toList.map (c => getReference(references, toCheck = c) ), ":")
    results
  }
    
  def outputFormat(list : List[Reference], separator: String) : List[String]= {
    list.map { l =>
      "key["+ l.key +"]" + separator + "occurences["+ l.occurencesOpt.getOrElse(-1) + "]" + separator + "indice[" + l.indiceOpt.getOrElse(-1) + "]"
    }
  }
  /**
   * Return per candidateLine the max by occurences sum (sign, sum)
   */
  def checkBestProfileSignPerCandidateLine(candidateLine : String, referencesList : Map[String, Map[String, KeyOccurences]]) : (String, Int) = {
    val sumOccurencesPerSign = referencesList.map(f => {
      val sign = f._1
      (sign, sumOccurences(candidateLine , references = f._2))
    })
    //get the max sum of occurences beetween signs
    sumOccurencesPerSign.maxBy(f=> f._2)
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
    
    /*val toCheck1 = "0G "
    printKeySearch(filename = filename4, keyToSearch = toCheck1, reference = getReference(sortedDescKeyByOccurences = keyOccurences, toCheck1))
    */
  
    //store candidates - distinct elements
    val candidateLinesRDD = ctx.textFile(path="/home/maya/data/fidmarques/d_export_candidate.txt", minPartitions=3)
    candidateLinesRDD.distinct().cache()
    
    def saveParseCandidatesWithReferences(ctx : SparkContext, toSaves : List[String], resultFilename : String) = {
      val results = ctx.parallelize(seq = toSaves, numSlices = 1)
      results.saveAsTextFile(resultFilename)
    }
    
    val filename4 = "/home/maya/data/fidmarques/d_export_4.txt"
    val references4RDD = ctx.parallelize(seq=Seq(getKeysWithOccurences(ctx, filename4)), numSlices=10).cache()
    //val results4 = parseCandidatesTrigrammeWithReference(candidateLinesRDD, references = references4RDD.first().sortedDescKeyByOccurences)
    //saveParseCandidatesWithReferences(ctx, results4, "/home/maya/data/fidmarques/resultFrom4.txt")
    
    val filename16 = "/home/maya/data/fidmarques/d_export_16.txt"
    val references16RDD = ctx.parallelize(seq=Seq(getKeysWithOccurences(ctx, filename16)), numSlices=10).cache()
    //val results16 = parseCandidatesTrigrammeWithReference(candidateLinesRDD, references = references16RDD.first().sortedDescKeyByOccurences)
    //saveParseCandidatesWithReferences(ctx, results16, "/home/maya/data/fidmarques/resultFrom16.txt")
    
    val referencesList = Map(
        "enseigne4"-> references4RDD.toLocalIterator.toList.head.sortedDescKeyByOccurences,
        "enseigne16"-> references16RDD.toLocalIterator.toList.head.sortedDescKeyByOccurences
    )
    
    //check which sign best approaches the ticket regarding to its lines (of candidates - products) 
    val bestSignPerCandidates = candidateLinesRDD.map { line => (line , checkBestProfileSignPerCandidateLine(candidateLine = line, referencesList)) }
    val groupBySigns = bestSignPerCandidates.groupBy(f => f._2._1, numPartitions=5)
    val numberPerSigns = groupBySigns.map(f=> (f._1, f._2.toList.size))
    val totalDistinct = candidateLinesRDD.toLocalIterator.size
    numberPerSigns.foreach(f=> println(" *** enseigne(%s) - number of candidates/totalDistinct (%s)/(%s)".format(f._1, f._2, totalDistinct)))
    
    
    ctx.stop()
  }
}