package org.apache.spark.secco.errors

object QueryCompilationErrors {

  // edited by lgh: AnalysisException -> Exception, "the pattern ..." -> "AnalysisException: the pattern ..."
  def invalidPatternError(pattern: String, message: String): Throwable = {
    new Exception(
      s"AnalysisException: the pattern '$pattern' is invalid, $message")
  }

}
