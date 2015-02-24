package com.rv.parser

object Time {
  // measures time taken by enclosed code
  def timed[A](block: => A) = {
    val t0 = System.currentTimeMillis
    val result = block
    println("took " + (System.currentTimeMillis - t0) + "ms")
    result
  }
}