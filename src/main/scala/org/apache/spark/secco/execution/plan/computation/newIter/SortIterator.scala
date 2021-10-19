package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.spark.secco.execution.storage.block.InternalBlock
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

import java.util.Comparator

/** The base class for building index. */
sealed abstract class BaseSortIterator extends BlockingSeccoIterator {

  /** The function to order the tuples */
  def sortFunction: Comparator[InternalRow]
}

/** The blocking iterator that sort the output of the child iterator
  * @param childIter the child iterator
  * @param localAttributeOrder the lexcial order to sort the output of the child iterator
  * @param sortOrder the sort direction of each position in lexcial order, i.e., "greater than" or "less than",
  *                  true means "less than", false means "greater than"
  */
case class SortIterator(
    childIter: SeccoIterator,
    localAttributeOrder: Array[Attribute],
    sortOrder: Array[Boolean]
) extends BaseSortIterator {

  override def isSorted(): Boolean = ???

  override def results(): InternalBlock = ???

  override def children: Seq[SeccoIterator] = childIter :: Nil

  override def sortFunction: Comparator[InternalRow] = ???
}
