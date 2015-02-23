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
        println("files number %s".format(files.length)) 
        files.length match {
          case n if n> 0 => Some(files.map { m => m.getAbsolutePath })
          case _ => None
        }
      }
      case false => None
    }
  }
  
  def isTextFile(filename: String) = {
    Seq("file",  filename).lineStream.toIterable.toList.mkString.contains("text")
  }
 
  def getLines(filename : String) = {
    scala.io.Source.fromFile(filename, "utf-8").getLines.toList
  }
  
  // a parallel filter lines
  def filterLines(lines : List[String], pattern : String) = {
    lines.par.filter { line => line.contains(pattern) }.toList
  }
  
  
  //on each line count the number of occurences
  def countSubstring(str1:String, str2:String):Int={
   @tailrec def count(pos:Int, c:Int):Int={
      val idx=str1 indexOf(str2, pos)
      if(idx == -1) c else count(idx+str2.size, c+1)
   }
   count(0,0)
  }
  
  
  def doTask(pathDir : String, pattern : String) = {
    val filePathsOpt = getFilePaths(pathDir)
    if (filePathsOpt.isDefined) {
      filePathsOpt.get.map { f => {
        val lines = getLines(filename=f)
        
        //for each list retain only lines with pattern
        val filteredLines = filterLines(lines, pattern)
        
        //count the number of occurences of pattern on each line in parallel mode
        val counts = filteredLines.par.map(m=>{
        	countSubstring(str1=m, str2 = pattern)
        })
        val count = counts.foldLeft(0L)(_+_)
        
        //val count = filteredLines.size
        println("logfile %s - pattern(%s) - occurences(%s)".format(f,pattern, count ))
        } }
    }else {
      println("No files found!!")
    }
  }
  
  def main(args: Array[String]) {
    //val pathDir = "/home/maya/Downloads/apache-sample-rar_log/access_log"
    //val pathDir = "/home/maya/play_gambling/front_ui/logs"
    val pathDir = "/home/maya/akka-mine-sample"
      doTask(pathDir, pattern = "de")
    }

}