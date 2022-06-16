package org.apache.spark.secco.util.counter

import java.util.concurrent.atomic.AtomicLong

/** The counter with `Long` value.
  * @param scope scope of the counter.
  * @param name name of the counter
  */
case class Counter(scope: String, name: String) {
  final private val _counter = new AtomicLong(0)

  /** Value of counter. */
  def value: Long = _counter.get

  /** Modify the value by x amount. */
  private def modifyValue(x: Long): Unit = {
    while ({
      true
    }) {
      val existingValue = value
      val newValue = existingValue + x
      if (_counter.compareAndSet(existingValue, newValue)) return
    }
  }

  def next(): Long = {
    val old_value = value
    increment(1)
    old_value
  }

  /** Increment counter by 1. */
  def increment(): Unit = modifyValue(1)

  /** Increment counter by x. */
  def increment(x: Long): Unit = {
    assert(x >= 0, "amount to be incremented must be >= 0")
    modifyValue(x)
  }

  /** Decrement counter by 1. */
  def decrement(): Unit = modifyValue(-1)

  /** Decrement counter by x. */
  def decrement(x: Long): Unit = {
    assert(x >= 0, "amount to be decremented must be >= 0")
    modifyValue(-x)
  }

  /** Reset counter to 0. */
  def reset(): Unit = {
    while ({
      true
    }) {
      val existingValue = value
      val newValue = 0
      if (_counter.compareAndSet(existingValue, newValue)) return
    }
  }
}
