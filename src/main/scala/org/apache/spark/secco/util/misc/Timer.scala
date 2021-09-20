package org.apache.spark.secco.util.misc

class Timer {

  var time1 = System.currentTimeMillis()
  var time2 = System.currentTimeMillis()

  def start() = {
    time1 = System.currentTimeMillis()
  }

  def end() = {
    time2 = System.currentTimeMillis()
  }

  override def toString: String = {
    (time2 - time1).toString
  }

}
