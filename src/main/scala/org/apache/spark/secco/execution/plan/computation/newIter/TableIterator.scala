package org.apache.spark.secco.execution.plan.computation.newIter
import org.apache.commons.lang.NotImplementedException
import org.apache.spark.secco.execution.storage.block.{IndexLike, InternalBlock, MapLike, SetLike, TrieLike}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute

/** The base class for table iterator */
sealed abstract class BaseTableIterator extends SeccoIterator {

  /** The underlying [[InternalBlock]] that supports the iterator */
  def block(): InternalBlock

}

/** The table iterator of the none index like [[InternalBlock]], i.e., [[GenericInternalBlock]],
  * [[UnsafeInternalBlock]]
  */
case class TableIterator(
    block: InternalBlock,
    localAttributeOrder: Array[Attribute],
    isSorted: Boolean
) extends BaseTableIterator {

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = block

  override def hasNext: Boolean = block.iterator.hasNext

  override def next(): InternalRow = block.iterator.next()

  override def children: Seq[SeccoIterator] = Seq()
}

/** The table iterator of index like [[InternalBlock]] */
case class IndexableTableIterator(
    block: InternalBlock with IndexLike,
    keyAttributes: Array[Attribute],
    localAttributeOrder: Array[Attribute],
    isSorted: Boolean
) extends BaseTableIterator
    with IndexableSeccoIterator {

  override def setKey(key: InternalRow): Boolean = {
    block match {
      case mapBlock: MapLike => mapBlock.contains(key)
      case trieBlock: TrieLike => trieBlock.containPrefix(key)
      case setBlock: SetLike => setBlock.contains(key)
      case _ => throw new NotImplementedException()  //lgh TODO: may consider other types of exception
    }
  }

  override def getOneRow(key: InternalRow): Option[InternalRow] = {
    if(setKey(key))
      block match {
        case mapBlock: MapLike => Some(mapBlock.get(key))
        case trieBlock: TrieLike => Some(trieBlock.getRows(key).head)
//        case setBlock: SetLike => _ //lgh: TODO:
        case _ => throw new NotImplementedException()  //lgh TODO: may consider other types of exception
      }
    else
      None
  }

  override def unsafeGetOneRow(key: InternalRow): InternalRow = {
    if(setKey(key))
      block match {
        case mapBlock: MapLike => mapBlock.get(key)
        case trieBlock: TrieLike => trieBlock.getRows(key).head
//        case setBlock: SetLike => _ //lgh: TODO:
        case _ => throw new NotImplementedException()  //lgh TODO: may consider other types of exception
      }
    else
      null
  }

  override def isBreakPoint(): Boolean = false

  override def results(): InternalBlock = block

  override def hasNext: Boolean = block.iterator.hasNext

  override def next(): InternalRow = block.iterator.next()

  override def children: Seq[SeccoIterator] = Seq()
}
