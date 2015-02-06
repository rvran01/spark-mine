package com.rv.spark.sample

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

case class RateOperation(userId : Int, movieId : Int, ratingId : Int, operationTime : Long)
case class Movie(movieId: Int, title : String, groups: String)

object SparkMovieRating {
  //sorted the movieId by highest ratings amount
  def main(args: Array[String]): Unit = {
    
    val filenameRatings = "/home/maya/data/movie-ml-1m/ratings.dat"
  
    val sparkConf = new SparkConf()
      .set("spark.executor.memory", "4g")
      .set("SPARK_LOCAL_IP", "localhost")
      .setMaster("local[2]") //using 2 cores
      .setAppName("Spark Movie")
      
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(filenameRatings, 1)
    
    val rateOperations = lines.map(f => {
      val line = f.split("::")
      new RateOperation(userId = line(0).toInt, movieId = line(1).toInt, ratingId = line(2).toInt, operationTime = line(3).toLong)
    })
    val rateOperationsRdd = ctx.parallelize(seq=rateOperations.toLocalIterator.toSeq, numSlices=4)
    
    println("rateOperationsRdd.size %s".format(rateOperationsRdd.count))
    
    val grouped = rateOperations.groupBy(f=> f.movieId)
    val groupedRatingsByMovie = grouped.map(f=> (f._1, f._2.map(n=> n.ratingId)))
     groupedRatingsByMovie.cache
    val movieScorings = groupedRatingsByMovie.map(f=> (f._1, f._2.toList.foldLeft(0)(_+_)))
    val sortedMovieScorings = movieScorings.sortBy(f=> f._2, ascending = false, numPartitions=5)
    //sortedMovieScorings.foreach(f=> println("movieId(%s) ratings(%s)".format(f._1, f._2)))
   
    println(" ****>  groupedRatingsByMovie.size %s".format( groupedRatingsByMovie.count))
    sortedMovieScorings.cache()
    
    //associate the name of the top 10 ratings movie
    val filenameMovies = "/home/maya/data/movie-ml-1m/movies.dat"
    val lineMovies = ctx.textFile(filenameMovies, 1)
    
    val movies = lineMovies.map(f => {
      val line = f.split("::")
      new Movie(movieId = line(0).toInt, title = line(1), groups = line(2))
    })
    
    val moviesById = movies.map { m => (m.movieId -> (m.title, m.groups))}
    val maps = moviesById.toLocalIterator.toMap.map(l=> l._1 -> l._2)
    sortedMovieScorings.take(10).foreach{f => {
      val movieId = f._1
      val ratings = f._2
      val movie = maps(movieId)
      println("movieId(%s) - title(%s) - type(%s) - ratings(%s)".format(movieId, movie._1, movie._2, ratings))}
     
    }   
    ctx.stop
  }

}