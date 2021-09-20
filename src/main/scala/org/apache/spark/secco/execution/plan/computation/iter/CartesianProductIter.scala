package org.apache.spark.secco.execution.plan.computation.iter
import org.apache.spark.secco.execution.{InternalDataType, InternalRow}

case class CartesianProductIter(
    baseIt: SeccoIterator,
    indexIt: SeccoIterator,
    localAttributeOrder: Array[String]
) extends SeccoIterator {

  private val baseAttrAtOutputPos: Array[Int] =
    baseIt.localAttributeOrder.map(localAttributeOrder.indexOf)
  private val indexAttrAtOutputPos: Array[Int] =
    indexIt.localAttributeOrder.map(localAttributeOrder.indexOf)

  private val baseAttrSize: Int = baseIt.localAttributeOrder.length
  private val indexAttrSize: Int = indexIt.localAttributeOrder.length

  private val outputRow =
    new Array[InternalDataType](localAttributeOrder.length)

  private var isBaseInitialized = false

  // we assume that index iterator produce non-empty results by default
  private var isIndexNotEmpty = true

  // There is no need to implement reset, as in left-deep plan,
  // cartesian product iterator will never be used as indexIt.
  override def reset(prefix: InternalRow): SeccoIterator = ???

  @inline override def hasNext: Boolean = {

    if (!isBaseInitialized && baseIt.hasNext) {
      val baseRow = baseIt.next()
      var i = 0
      while (i < baseAttrSize) {
        outputRow(baseAttrAtOutputPos(i)) = baseRow(i)
        i += 1
      }
      isBaseInitialized = true
    }

    if (indexIt.hasNext) {
      true
    } else if (baseIt.hasNext && isIndexNotEmpty) {
      //reset using empty prefix to initiate full reset
      indexIt.reset(Array[InternalDataType]())

      if (indexIt.isEmpty) {
        isIndexNotEmpty = false
        return false
      }

      val baseRow = baseIt.next()
      var i = 0
      while (i < baseAttrSize) {
        outputRow(baseAttrAtOutputPos(i)) = baseRow(i)
        i += 1
      }
      true
    } else {
      false
    }

  }

  @inline override def next(): InternalRow = {
    val indexRow = indexIt.next()
    var i = 0
    while (i < indexAttrSize) {
      outputRow(indexAttrAtOutputPos(i)) = indexRow(i)
      i += 1
    }

    outputRow
  }
}
