package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.{
  GenericInternalRowBlock,
  InternalBlock,
  UnsafeInternalRowBlock
}
import org.apache.spark.secco.execution.storage.row.{
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.execution.storage.Utils.InternalRowComparator
import org.apache.spark.secco.types.StructType

import scala.collection.mutable.ArrayBuffer

/** The base class for performing distinct operation via iterator */
sealed abstract class BaseDistinctIterator extends SeccoIterator {}

/** The iterator that performs distinct operation
  *
  * Note that: whether this operator is blocking relies on whether the child iterator is sorted.
  */
case class DistinctIterator(childIter: SeccoIterator)
    extends BaseDistinctIterator {

  private var rowCache: InternalRow = InternalRow()
  private var rowCacheAssignedOnce: Boolean = false
  private var hasNextCache: Boolean = _
  private var hasNextCacheValid = false

  private val schema: StructType =
    StructType.fromAttributes(localAttributeOrder())

  private val comparator = new InternalRowComparator(schema)

  override def localAttributeOrder(): Array[Attribute] =
    childIter.localAttributeOrder()

  // lgh if childerIter.isSorted() == false, we will sort the rows in the method results()
//  override def isSorted(): Boolean = childIter.isSorted()
  override def isSorted(): Boolean = true

  override def isBreakPoint(): Boolean = !childIter.isSorted()

  override def results(): InternalBlock = {
    val rowArrayBuffer = ArrayBuffer[InternalRow]()
    val rows = childIter.results().toArray()
    java.util.Arrays.sort(rows, comparator)
    val rowIter = rows.toIterator
    if (!rowIter.hasNext) return InternalBlock(Array(), schema)
    var rowTemp1 = rowIter.next()
    rowArrayBuffer += rowTemp1.copy()
    while (rowIter.hasNext) {
      val rowTemp2 = rowIter.next()
      val encounterNewRow = comparator.compare(rowTemp2, rowTemp1) != 0
      if (encounterNewRow) {
        rowArrayBuffer += rowTemp2
        rowTemp1 = rowTemp2
      }
    }
    InternalBlock(rowArrayBuffer.toArray, schema)
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil

  override def hasNext: Boolean = {
    if (isBreakPoint()) throw new NoSuchMethodException()
    if (!hasNextCacheValid) {
      if (!rowCacheAssignedOnce && childIter.hasNext) {
        rowCache = childIter.next()
        rowCacheAssignedOnce = true
        hasNextCache = true
        hasNextCacheValid = true
        return true
      }
      var encounterNewRow = false
      while (!encounterNewRow && childIter.hasNext) {
        val row_temp = childIter.next()
        encounterNewRow = comparator.compare(row_temp, rowCache) != 0
        if (encounterNewRow)
          rowCache = row_temp
      }

      hasNextCache = encounterNewRow
      hasNextCacheValid = true
    }
    hasNextCache
  }

  override def next(): InternalRow = {
    if (isBreakPoint()) throw new NoSuchMethodException()
    if (!hasNext) throw new NoSuchElementException("next on empty iterator")
    else {
      hasNextCacheValid = false
      UnsafeInternalRow.fromInternalRow(schema, rowCache)
    }
  }
}

/** The iterator that performs distinct operation and support index-like operations. */
case class IndexableDistinctIterator(
    childIter: SeccoIterator with IndexableSeccoIterator,
    keyAttributes: Array[Attribute]
) extends BaseDistinctIterator
    with IndexableSeccoIterator {

  override def setKey(key: InternalRow): Boolean = childIter.setKey(key)

  override def getOneRow(key: InternalRow): Option[InternalRow] =
    childIter.getOneRow(key)

  override def unsafeGetOneRow(key: InternalRow): InternalRow =
    childIter.unsafeGetOneRow(key)

  override def localAttributeOrder(): Array[Attribute] =
    childIter.localAttributeOrder()

  override def isSorted(): Boolean = ??? // same with above

  override def isBreakPoint(): Boolean = !childIter.isSorted()

  override def results(): InternalBlock = ??? // same with above

  override def children: Seq[SeccoIterator] = childIter :: Nil

  override def hasNext: Boolean = ??? // same with above

  override def next(): InternalRow = ??? // same with above
}

//lgh backup code segment

//1)
//override def hasNext: Boolean = {
//  if(isBreakPoint())  throw new NoSuchMethodException()
//  if(!hasNextCacheValid)
//  {
//  var encounterNewRow = false
//  while(!encounterNewRow && childIter.hasNext){
//  val row_temp = childIter.next()
//  //lgh
//  encounterNewRow = !localAttributeOrder().map(_.asInstanceOf[AttributeReference]).
//  zipWithIndex.map( t => row_temp.get(t._2, t._1.dataType) == rowCache.get(t._2, t._1.dataType) ).reduce(_&&_)
//  //        encounterNewRow = !row_temp.e
//  }
//  hasNextCache = encounterNewRow
//  hasNextCacheValid = true
//  }
//  hasNextCache
//  }
