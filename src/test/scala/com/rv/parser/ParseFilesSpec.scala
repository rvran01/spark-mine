package com.rv.parser
import org.scalatest.FlatSpec 

class ParseFilesSpec extends FlatSpec {

  it should "checkfile  file text" in {
    assert(!ParseFiles.isTextFile(filename="activator-launch-1.2.12.jar")) 
    assert(ParseFiles.isTextFile(filename="README.md"))
  }
  
  it should "count substring in string" in {
    assert(3 == ParseFiles.countSubstringInString(toParse="hello el grigel", toFind="el"))
    assert(0 == ParseFiles.countSubstringInString(toParse="hello el grigel", toFind="or"))
    assert(0 == ParseFiles.countSubstringInString(toParse="hello el grigel", toFind=""))
  }
  
  it should "recursive countSubstring method same as countSubstringInString" in {
    assert(ParseFiles.countSubstringInString(toParse="hello el grigel", toFind="el")
        .equals(ParseFiles.countSubstring(str1="hello el grigel", str2="el")))
    assert(ParseFiles.countSubstringInString(toParse="hello el grigel", toFind="or")
        .equals(ParseFiles.countSubstring(str1="hello el grigel", str2="or")))
    assert(ParseFiles.countSubstringInString(toParse="lore ipsum barba truc.orn nor licorn", toFind="or")
        .equals(ParseFiles.countSubstring(str1="lore ipsum barba truc.orn nor licorn", str2="or")))
  }

}