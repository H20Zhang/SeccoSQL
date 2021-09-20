package unit.utils.counter

import org.apache.spark.secco.util.counter.Counter
import org.scalatest.FunSuite

import scala.util.Try

class CounterSuite extends FunSuite {

  test("check initialization") {
    val counter = Counter("x", "y")
  }

  test("check counter's operation in single-threaded environment") {
    val counter = Counter("x", "y")

    for (i <- 0 until 100) {
      counter.increment()
    }

    assert(counter.value == 100)

    for (i <- 0 until 200) {
      counter.decrement()
    }

    assert(counter.value == -100)

    counter.increment(1000)
    assert(counter.value == 900)

    counter.decrement(2000)
    assert(counter.value == -1100)

    assert(Try(counter.increment(-100)).isFailure)
    assert(Try(counter.decrement(-100)).isFailure)
  }

  test("check counter's operation in multi-threaded environment") {

    val counter = Counter("x", "y")

    Range(0, 100).par.foreach { _ => counter.increment() }

    assert(counter.value == 100)

    Range(0, 200).par.foreach { _ => counter.decrement() }

    assert(counter.value == -100)

  }

}
