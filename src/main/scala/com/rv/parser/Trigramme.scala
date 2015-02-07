package com.rv.parser

object Trigramme {
  
	def perThree(line: String) = {
    val elt = line.toLowerCase()
		val numberTrig = line.length()-2
		var list = List.empty[String]
		for (i <- 0 to numberTrig-1){
			val current  =  (""+ elt.charAt(i) + elt.charAt(i+1) + elt.charAt(i+2))
				list = list ++ List(current)
			}
		list
	}
  
  def decoupe(line: String, size: Int) = {
    line.toLowerCase().sliding(size, step=1).toList
  }
}