//package org.apache.spark.secco.execution.plan.computation.iter
//import org.apache.spark.secco.execution.OldInternalRow
//import org.apache.spark.secco.execution.plan.computation.utils.{
//  Accessor,
//  ConsecutiveRowArray,
//  InternalRowHashMap,
//  Trie
//}
//
//trait TableIter extends SeccoIterator {}
//
//case class TrieTableIter(trie: Trie, localAttributeOrder: Array[String])
//    extends TableIter {
//
//  private val trieAccessor: Accessor = trie.access()
//
//  private var underlyingIter: Iterator[OldInternalRow] = trieAccessor.iterator()
//
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//
//    underlyingIter = trieAccessor.indexIterator(prefix)
//
//    this
//  }
//
//  @inline override def hasNext: Boolean = {
//    underlyingIter.hasNext
//  }
//
//  @inline override def next(): OldInternalRow = {
//    underlyingIter.next()
//  }
//}
//
//case class ArrayTableIter(
//    array: Array[OldInternalRow],
//    localAttributeOrder: Array[String]
//) extends TableIter {
//
//  private var underlyingIter: Iterator[OldInternalRow] = array.iterator
//
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    if (prefix.size == 0) {
//      underlyingIter = array.iterator
//      this
//    } else {
//      throw new Exception(
//        s"${getClass} only support full reset, given prefix:${prefix.toSeq}"
//      )
//    }
//  }
//
//  @inline override def hasNext: Boolean = {
//    underlyingIter.hasNext
//  }
//
//  @inline override def next(): OldInternalRow = {
//    underlyingIter.next()
//  }
//}
//
//case class ConsecutiveRowArrayTableIter(
//    array: ConsecutiveRowArray,
//    localAttributeOrder: Array[String]
//) extends TableIter {
//
//  private var underlyingIter: Iterator[OldInternalRow] = array.iterator
//
//  //TODO: fix this
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    if (prefix.size == 0) {
//      underlyingIter = array.iterator
//      this
//    } else {
//      throw new Exception(
//        s"${getClass} only support full reset, given prefix:${prefix.toSeq}"
//      )
//    }
//  }
//
//  @inline override def hasNext: Boolean = {
//    underlyingIter.hasNext
//  }
//
//  @inline override def next(): OldInternalRow = {
//    underlyingIter.next()
//  }
//}
//
//case class HashMapTableIter(
//    hashMap: InternalRowHashMap,
//    localAttributeOrder: Array[String]
//) extends TableIter {
//
//  private var underlyingArray: Array[OldInternalRow] = Array.empty
//  var i = 0
//  var arraySize = 0
//
//  /** reset the iterator based on prefix */
//  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    underlyingArray = hashMap.get(prefix)
//    i = 0
//    arraySize = underlyingArray.size
//    this
//  }
//
//  override def hasNext: Boolean = i < arraySize
//
//  override def next(): OldInternalRow = {
//    val row = underlyingArray(i)
//    i += 1
//    row
//  }
//}
