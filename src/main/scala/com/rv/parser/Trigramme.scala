package com.rv.parser

object Trigramme {
  
	def tri(line: String) = {
		val numberTrig = line.length()-2
		var list = List.empty[String]
		for (i <- 0 to numberTrig-1){
			val current  =  (""+ line.charAt(i) + line.charAt(i+1) + line.charAt(i+2))
				list = list ++ List(current)
			}
		list
	}

}