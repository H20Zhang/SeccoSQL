package org.apache.spark.secco.execution.plan.computation.newIter

import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.block.{HashMapInternalBlock, HashSetInternalBlock, InternalBlock, TrieInternalBlock}
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.codegen.GenerateSafeProjection
import org.apache.spark.secco.types.StructType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** The base class for building index. */
sealed abstract class BaseBuildIndexIterator extends BlockingSeccoIterator {

  /** build the index and convert it to a IndexableSeccoIterator */
  def toIndexIterator(): SeccoIterator with IndexableSeccoIterator

  override def isBreakPoint(): Boolean = true
}

case class BuildTrie(
    childIter: SeccoIterator,
    localAttributeOrder: Array[Attribute]
) extends BaseBuildIndexIterator {

  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    IndexableTableIterator(results().asInstanceOf[TrieInternalBlock], localAttributeOrder, localAttributeOrder, true)

  override def isSorted(): Boolean = true

  override def results(): InternalBlock = {
    val schema = StructType.fromAttributes(localAttributeOrder)
    TrieInternalBlock(childIter.results().toArray(), schema)
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

case class BuildHashMap(childIter: SeccoIterator, keyAttrs: Array[Attribute])
    extends BaseBuildIndexIterator {

  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    IndexableTableIterator(results(), keyAttrs, localAttributeOrder(), isSorted())

  override def localAttributeOrder(): Array[Attribute] = childIter.localAttributeOrder()

  override def isSorted(): Boolean = childIter.isSorted()

  override def results(): HashMapInternalBlock ={
    val rows = if(childIter.isBreakPoint()){
      childIter.results().toArray()
    }else{
      val buffer = ArrayBuffer[InternalRow]()
      while(childIter.hasNext) buffer.append(childIter.next())
      buffer.toArray
    }
    HashMapInternalBlock(rows, childIter.localAttributeOrder(), keyAttrs)
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}

case class BuildSet(childIter: SeccoIterator, keyAttrs: Array[Attribute])
extends BaseBuildIndexIterator {
  override def toIndexIterator(): SeccoIterator with IndexableSeccoIterator =
    IndexableTableIterator(results().asInstanceOf[HashSetInternalBlock], keyAttrs, localAttributeOrder(), isSorted())

  override def localAttributeOrder(): Array[Attribute] = childIter.localAttributeOrder()

  override def isSorted(): Boolean = childIter.isSorted()

  override def results(): InternalBlock = {
//    val schema = Utils.attributeArrayToStructType(localAttributeOrder())
    val schema = StructType.fromAttributes(localAttributeOrder())
    new HashSetInternalBlock(mutable.HashSet[InternalRow](childIter.results().toArray(): _*), schema)
    //lgh TODO: check whether the implementation of contains(InternalRow) is corrected in this class
    //lgh TODO: check whether "keyAttrs" should be used here and whether the implementation of the initialization of HashSetInternalBlock is correct.
  }

  override def children: Seq[SeccoIterator] = childIter :: Nil
}
