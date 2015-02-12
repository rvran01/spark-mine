package com.rv.spark.sample

import org.scalatest.FlatSpec 

class SparkOccurenceCounterSpec extends FlatSpec {

  
	val references = Map(
			"SMA"-> KeyOccurences(key="SMA", occurences =5, indice =1),
			"MAR"-> KeyOccurences(key="MAR", occurences =2, indice =2),
			"TIE"-> KeyOccurences(key="TIE", occurences =1, indice =3),
			"NOE"-> KeyOccurences(key="NOE", occurences =1, indice =4),
			"OEL"-> KeyOccurences(key="OEL", occurences =1, indice =5)
			)
  it should "ok for sum occurences of trigramme" in {
    val res1 = SparkOccurenceCounter.sumOccurences(candidateLine = "SMARTIES LE P NOEL", references : Map[String, KeyOccurences])
    assert(10 == res1)
    val res2 = SparkOccurenceCounter.sumOccurences(candidateLine = "TIES LE", references : Map[String, KeyOccurences])
    assert(1 == res2)
    val res3 = SparkOccurenceCounter.sumOccurences(candidateLine = "LE P NOEL", references : Map[String, KeyOccurences])
    assert(2 == res3)
  }
  
  it should "be 0 as trigrammes not found in references" in {
    val res = SparkOccurenceCounter.sumOccurences(candidateLine = "SM LE P", references : Map[String, KeyOccurences])
    assert(0 == res)
  }
}