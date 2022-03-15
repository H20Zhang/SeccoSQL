//package org.apache.spark.secco.execution.plan.computation.iter
//
//import org.apache.spark.secco.execution.OldInternalRow
//
//import scala.collection.Iterator.empty
//
//case class SelectIter(
//    childIter: SeccoIterator,
//    selectionExecFunc: OldInternalRow => Boolean,
//    localAttributeOrder: Array[String]
//) extends SeccoIterator {
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    hdDefined = false
//    hd = null
//    childIter.reset(prefix)
//    this
//  }
//
//  private var hd: OldInternalRow = _
//  private var hdDefined: Boolean = false
//
//  @inline def hasNext: Boolean =
//    hdDefined || {
//      do {
//        if (!childIter.hasNext) return false
//        hd = childIter.next()
//      } while (!selectionExecFunc(hd))
//      hdDefined = true
//      true
//    }
//
//  @inline def next(): OldInternalRow =
//    if (hasNext) { hdDefined = false; hd }
//    else empty.next()
//}
