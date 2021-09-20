package org.apache.spark.secco.execution.storage.block

import org.apache.spark.secco.execution.storage.row.InternalRow

/** The base class for builder for InternalBlock */
abstract class InternalBlockBuilder {

  /** Add a new row to builder. */
  def add(row: InternalRow): Unit

  /** Build the InternalBlock. */
  def build(): InternalBlock
}
