package org.apache.spark.secco.util

object DebugUtils {

  def printlnDebug(str: String) = {
    println(s"[debug]: + $str")
  }

  def relaxedStringEqual(
      obj1: Any,
      obj2: Any,
      ignore: Seq[String] = Seq("\n")
  ): Boolean = {

    var outputString = obj1.toString.diff(obj2.toString)

    ignore.foreach(ignoreString =>
      outputString = outputString.replaceAll(ignoreString, "")
    )

    outputString.isEmpty
  }

}
