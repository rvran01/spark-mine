package com.rv.parser

import org.scalatest.FlatSpec 

class TrigrammeSpec extends FlatSpec {

  it should "contain 9 elements in list" in {
    assert(9 == (Trigramme.perThree("hello world").size))    
  }
  
  it should "contain 1 elements in list" in {
    assert(1 == (Trigramme.perThree("HEL").size))    
  }
  
  it should "contain 9 elements in list after decoupe" in {
    val res = Trigramme.decoupe("hello world", 3)
    assert(9 == (res.size))
    assert(res.contains("HEL"))
    assert(res.contains("ELL"))
    assert(res.contains("LLO"))
    assert(res.contains("LO "))
    assert(res.contains("O W"))
    assert(res.contains(" WO"))
    assert(res.contains("RLD"))
  }
  
  it should "contain the same elements : methods decoupe and perThree" in {
    val line = "hello world"
    val resDecoupe = Trigramme.decoupe(line, 3)
    val resTri = Trigramme.perThree(line)
    assert(resDecoupe.equals(resTri))
    
  }
}