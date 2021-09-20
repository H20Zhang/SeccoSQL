package org.apache.spark.dolphin.execution.storage.block

import org.apache.spark.dolphin.execution.storage.row.InternalRow
import org.apache.spark.dolphin.types.StructType

/** The base class for builder for InternalBlock */
abstract class InternalBlockBuilder {

  /** Add a new row to builder. */
  def add(row: InternalRow): Unit

  /** Build the InternalBlock. */
  def build(): InternalBlock
}
