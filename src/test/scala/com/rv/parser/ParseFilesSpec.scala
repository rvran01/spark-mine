package com.rv.parser
import org.scalatest.FlatSpec 

class ParseFilesSpec extends FlatSpec {

  it should "checkfile  file text" in {
    assert(!ParseFiles.isTextFile(filename="activator-launch-1.2.12.jar")) 
    assert(ParseFiles.isTextFile(filename="README.md"))
  }

}