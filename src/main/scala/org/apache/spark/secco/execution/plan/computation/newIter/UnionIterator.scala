package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.{Ascending, Attribute, AttributeReference, BaseOrdering, BoundReference, SortOrder}
import org.apache.spark.secco.expression.codegen.GenerateOrdering
import org.apache.spark.secco.types.StructType

/** The base class for performing union via iterator. */
sealed abstract class BaseUnionIterator extends SeccoIterator {}

/** The iterator that performs union.
  *
  * Note: the sort order is maintained.
  */
case class UnionIterator(left: SeccoIterator, right: SeccoIterator)
    extends BaseUnionIterator {

  //lgh TODO: to make this iterator support two compatible children but with different order of Attributes in their localAttributeOrder()
  assert(left.localAttributeOrder().zip(right.localAttributeOrder()).map(t=>t._1.equals(t._2)).reduce(_&&_),
    "currently, localAttributeOrder of the two children of UnionIterator must be the same")

  private var leftRowCache: InternalRow = _
  private var rightRowCache: InternalRow = _
  private var leftRowCacheValid: Boolean = false
  private var rightRowCacheValid: Boolean = false
  private var nextIsLeft: Boolean = _

  lazy val ordering: Array[SortOrder] = localAttributeOrder().zipWithIndex.map {
    case(attr, index) =>
      SortOrder(BoundReference(index, attr.asInstanceOf[AttributeReference].dataType, nullable = true), Ascending)
  }

  lazy val comparator: BaseOrdering = GenerateOrdering.generate(ordering)
//
//  if (isSorted()) {
//    if (left.hasNext) {
//      leftRowCache = left.next();
//      leftRowCacheValid = true;
//      nextIsLeft = true
//    }
//    if (right.hasNext) {
//      rightRowCache = right.next();
//      rightRowCacheValid = true;
//      if(!leftRowCacheValid || comparator.compare(leftRowCache, rightRowCache) > 0) nextIsLeft = false
//    }
//  }

  override def localAttributeOrder(): Array[Attribute] = left.localAttributeOrder()

  override def isSorted(): Boolean = left.isSorted() && right.isSorted()

  override def isBreakPoint(): Boolean = false

  // lgh: sort is implemented in the merge method
  override def results(): InternalBlock = left.results().merge(right.results(), maintainSortOrder = isSorted())

  override def children: Seq[SeccoIterator] = left :: right :: Nil

  override def hasNext: Boolean = if (!isSorted()) left.hasNext || right.hasNext else {
    if (!leftRowCacheValid && left.hasNext) {
      leftRowCache = left.next();
      leftRowCacheValid = true;
    }
    if (!rightRowCacheValid && right.hasNext) {
      rightRowCache = right.next();
      rightRowCacheValid = true;
    }
    nextIsLeft = {
      if(rightRowCacheValid && (!leftRowCacheValid || comparator.compare(leftRowCache, rightRowCache) > 0))
        false
      else
        true
    }
    leftRowCacheValid || rightRowCacheValid
  }

  override def next(): InternalRow = {
    if(!hasNext) throw new NoSuchElementException("next on empty iterator")
    if (!isSorted()) {if (left.hasNext) left.next() else right.next()} else{
    val outRow =
      if (nextIsLeft) {
        leftRowCacheValid = false
  //        if(left.hasNext) {leftRowCache = left.next(); leftRowCacheValid = true}
        leftRowCache
      }
      else{
        rightRowCacheValid = false
  //        if(left.hasNext) {leftRowCache = left.next(); leftRowCacheValid = true}
        rightRowCache
      }
    UnsafeInternalRow.fromInternalRow(StructType.fromAttributes(localAttributeOrder()), outRow)
//    nextIsLeft =
//    if(!leftRowCacheValid || rightRowCacheValid && comparator.compare(leftRowCache, rightRowCache) > 0) false else true
//    outRow
  }
  }
}
