package com.rv.parser
import org.scalatest.FlatSpec

class TimeSpec extends FlatSpec{
  it should "show time in ms for function" in {
    println("**>(%s)".format(Time.timed(1)))
  }

}