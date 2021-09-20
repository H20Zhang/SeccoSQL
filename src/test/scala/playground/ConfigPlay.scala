package playground

import org.apache.spark.secco.config.SeccoConfiguration
import org.scalatest.FunSuite

class ConfigPlay extends FunSuite {

  test("basic") {

    val conf = SeccoConfiguration.newDefaultConf()
    println(conf.pairMemoryBudget)
    println(conf.timeoutDuration)
    conf.set("secco.local.cache_size", "1000")
    println(conf.getInt("secco.local.cache_size"))
    pprint.pprintln(Seq(1, 2, 3, 4))

    val x = new Array[AnyVal](2)
    x(0) = 1
    x(1) = 1.0

    pprint.pprintln(x)

    val y = 121312312312312312L
    val z = y.asInstanceOf[Double]
    val y1 = z.asInstanceOf[Long]
    println(y)
    println(y1)
  }

  test("metering") {

    val cardinality = 10000000
    val DoubleData =
      Range(0, cardinality).map(f => (f.toDouble, (f + 1).toDouble)).toArray
    val LongData =
      Range(0, cardinality).map(f => (f.toLong, (f + 1).toLong)).toArray

    import org.scalameter._
    val doubleTime = config(
      Key.exec.benchRuns -> 20
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val size = DoubleData.size
      var result = true
      var i = 0
      while (i < size) {
        val res = DoubleData(i)
        val l = res._1
        val r = res._2
        result =
          l < r & l < r & l < r & l < r & l < r & l < r & l < r & l < r & l < r
        i += 1
      }
      println(result)
    }

    val longTime = config(
      Key.exec.benchRuns -> 20
    ) withWarmer {
      new Warmer.Default
    } withMeasurer {
      new Measurer.IgnoringGC
    } measure {
      val size = LongData.size
      var result = true
      var i = 0
      while (i < size) {
        val res = LongData(i)
        val l = res._1
        val r = res._2
        result =
          l < r & l < r & l < r & l < r & l < r & l < r & l < r & l < r & l < r
        i += 1
      }
      println(result)
    }

    println(s"doubleTime: $doubleTime, longTime:$longTime")
  }

}
