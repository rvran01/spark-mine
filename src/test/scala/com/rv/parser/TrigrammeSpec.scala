package com.rv.parser

import org.scalatest.FlatSpec 

class TrigrammeSpec extends FlatSpec {

  it should "contain 9 elements in list" in {
    assert(9 == (Trigramme.tri("hello world").size))    
  }
  
  it should "contain 1 element1 in list" in {
    assert(1 == (Trigramme.tri("hel").size))    
  }
  
}