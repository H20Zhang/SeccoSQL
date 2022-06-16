package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.storage.row.{
  InternalRow,
  UnsafeInternalRow
}

import java.io.IOException
import java.util

/** An iterator interface used to pull the output from generated function for multiple operators
  * (whole stage codegen).
  */
abstract class BufferedRowIterator {
  protected var currentRows = new util.LinkedList[InternalRow]
  // used when there is no column in output
  protected var unsafeRow = new UnsafeInternalRow(numFields = 0)
  private val startTimeNs = System.nanoTime
  var partitionIndex: Int = -1
  @throws[IOException]
  def hasNext: Boolean = {
    if (currentRows.isEmpty) processNext()
    !currentRows.isEmpty
  }
  def next: InternalRow = currentRows.remove

  /** Returns the elapsed time since this object is created. This object represents a pipeline so
    * this is a measure of how long the pipeline has been running.
    */
  def durationMs(): Long = (System.nanoTime - startTimeNs) / (1000 * 1000)

  /** Initializes from array of iterators of InternalRow.
    */
  def init(index: Int, iters: Array[Iterator[InternalRow]]): Unit

  /** Append a row to currentRows.
    */
  def append(row: InternalRow): Unit = { currentRows.add(row) }

  /** Returns whether `processNext()` should stop processing next row from `input` or not.
    *
    * If it returns true, the caller should exit the loop (return from processNext()).
    */
  def shouldStop: Boolean = !currentRows.isEmpty

//  /** Increase the peak execution memory for current task.
//    */
//  def incPeakExecutionMemory(size: Long): Unit = {
//    TaskContext.get.taskMetrics.incPeakExecutionMemory(size)
//  }

  /** Processes the input until have a row as output (currentRow).
    *
    * After it's called, if currentRow is still null, it means no more rows left.
    */
  @throws[IOException]
  protected def processNext(): Unit
}
