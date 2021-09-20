package org.apache.spark.secco.execution.plan.computation.iter

import org.apache.spark.secco.execution.InternalRow

import scala.collection.Iterator.empty

case class SelectIter(
    childIter: SeccoIterator,
    selectionExecFunc: InternalRow => Boolean,
    localAttributeOrder: Array[String]
) extends SeccoIterator {
  override def reset(prefix: InternalRow): SeccoIterator = {
    hdDefined = false
    hd = null
    childIter.reset(prefix)
    this
  }

  private var hd: InternalRow = _
  private var hdDefined: Boolean = false

  @inline def hasNext: Boolean =
    hdDefined || {
      do {
        if (!childIter.hasNext) return false
        hd = childIter.next()
      } while (!selectionExecFunc(hd))
      hdDefined = true
      true
    }

  @inline def next(): InternalRow =
    if (hasNext) { hdDefined = false; hd }
    else empty.next()
}
