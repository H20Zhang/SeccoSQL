//package playground

//import org.scalameter
//import org.scalameter._
//import org.scalatest.FunSuite
//
//class ScalaMeterPlay extends FunSuite {
//  test("simple") {
//    import org.scalameter._
//    val time = measure {
//      for (i <- 0 until 100000) yield i
//    }
//    println(s"Total time: $time")
//  }
//
//  test("basic2") {
//
//    val time = config(Key.exec.benchRuns -> 100) withWarmer {
//      new Warmer.Default
//    } withMeasurer {
//      new Measurer.Default
//    } measure {
//      for (i <- 0 until 100000) yield i
//    }
//    println(s"Total time: $time")
//  }
//
//  test("basic3") {
//    val mem = config(Key.exec.benchRuns -> 20) withMeasurer (new Measurer.MemoryFootprint) measure {
//      for (i <- 0 until 100000) yield i
//    }
//    println(s"Total memory: $mem")
//
//  }
//}
