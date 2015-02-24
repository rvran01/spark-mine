package com.rv.parser

import java.io.File
import scala.annotation.tailrec
import sys.process._

object ParseFiles {
  
  def getFilePaths(pathDir : String) = {
    val d = new File(pathDir)
    (d.exists && d.isDirectory) match {
      case true => {
        val files = d.listFiles().filter { f => f.isFile() && isTextFile(f.getAbsolutePath) }.toList
        println("number of files %s".format(files.length))
        files.length match {
          case n if n> 0 => Some(files.map { m => m.getAbsolutePath })
          case _ => None
        }
      }
      case false => None
    }
  }
  
  def isTextFile(filename : String) = {
    Seq("file",  filename).lineStream.toIterable.toList.mkString.contains("text")
  }
 
  def getLines(filename : String) = {
    scala.io.Source.fromFile(filename, "utf-8").getLines
  }
  
  // a parallelize filter lines - slice file in chunks if necessary to avoid too many memory loading
  def filterLines(lines : Iterator[String], pattern : String, chunkSize : Int = 128*1024) = {
    val iterator = lines.grouped(chunkSize)
    val elements = iterator.map { x => x.par.filter { line => line.contains(pattern) }.toList}
    //group all list
    elements.flatten
  }
  
  
  //on each line count the number of occurences
  def countSubstring(str1 : String, str2 : String) : Int={
   @tailrec def count(pos:Int, c:Int):Int={
      val idx=str1 indexOf(str2, pos)
      if(idx == -1) c else count(idx+str2.size, c+1)
   }
   count(0,0)
  }
  
  //other implementation of previous method
  def countSubstringInString(toParse : String, toFind : String) = {
    toFind.isEmpty() match {
      case true => 0
      case false => {
    	  val reduce = toParse.replaceAll(toFind, "")
    		(toParse.length() - reduce.length() ) / toFind.length()
      }
    }
  }
  
  
  def doTask(pathDir : String, pattern : String, chunkSize :Int= 1024*128) = {
    val start = System.currentTimeMillis
    getFilePaths(pathDir) match {
      case Some(filePaths) => {
        val totalNumbers  = filePaths.map { f => {
          val lines = getLines(f)
        
          //for each list retain only lines with pattern
          val filteredLines = filterLines(lines, pattern)
        
          //count the number of occurences of pattern on each line in parallel mode
          val countPerLine = filteredLines.grouped(chunkSize).map(m=>{ m.par.map{x=> countSubstring(x, pattern)}}.toList)
          val countOccurencesPerFile = countPerLine.flatten.foldLeft(0L)(_+_)
        
          println("logfile %s - pattern(%s) - occurences(%s)".format(f, pattern, countOccurencesPerFile))
          countOccurencesPerFile
          }
        }
        val finalNumberOfOccurence = totalNumbers.foldLeft(0L)(_+_)
        println("Total numbers of occurence (%s) : (%s) - from (%s) files - duration (%s) ms".format(pattern, finalNumberOfOccurence, totalNumbers.size, (System.currentTimeMillis-start)))
    
        }
      case None =>  println("No files found!!")
    }
  }
  
  def main(args: Array[String]) {

    val pathDir = "/home/maya/akka-mine-sample"
      doTask(pathDir, pattern = "95")
      doTask(pathDir, pattern = "de")
      doTask(pathDir, pattern = "2")
      doTask(pathDir, pattern = "1")
      doTask(pathDir, pattern = "e")
    }

}