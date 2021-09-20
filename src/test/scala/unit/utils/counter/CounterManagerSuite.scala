package unit.utils.counter

import org.apache.spark.secco.util.counter.CounterManager
import util.SeccoFunSuite

class CounterManagerSuite extends SeccoFunSuite {

  test("check initialization") {
    val counterManager = CounterManager.newDefaultCounterManager
  }

  test("check get and reset") {
    val counterManager = CounterManager.newDefaultCounterManager

    val counter1 = counterManager.getOrCreateCounter("x", "y")
    assert(counterManager.getOrCreateCounter("x", "y").eq(counter1))

    counter1.increment(100)
    assert(counter1.value == 100)

    counterManager.resetCounter("x", "y")
    assert(counter1.value == 0)

    val counter2 = counterManager.getOrCreateCounter("y")
    assert(counterManager.getOrCreateCounter("y").eq(counter2))

    counter2.increment(100)
    assert(counter2.value == 100)

    counterManager.resetCounter("y")
    assert(counter2.value == 0)

  }

}
