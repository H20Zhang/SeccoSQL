package org.apache.spark.secco.execution.plan.computation.iter

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}
import org.apache.spark.secco.execution.plan.computation.utils._

trait JoinIter extends SeccoIterator {}

case class GHDJoinIter(
    tries: Array[Trie],
    schemas: Seq[Seq[String]],
    localAttributeOrder: Array[String]
) extends JoinIter {

  lazy val lf: LeapFrogJoin = LeapFrogJoin(tries, localAttributeOrder, schemas)
  lazy val plf: PartialLeapFrogJoin =
    PartialLeapFrogJoin(tries, localAttributeOrder, schemas)
  var underlyingIter: Iterator[OldInternalRow] = lf

  override def reset(prefix: OldInternalRow): SeccoIterator = {
    plf.unsafeInit(prefix)
    underlyingIter = plf
    this
  }

  override def hasNext: Boolean = underlyingIter.hasNext

  override def next(): OldInternalRow = {
    val nextRes = underlyingIter.next()
    nextRes
  }
}

case class BinaryJoinIter(
    baseIt: SeccoIterator,
    indexIt: SeccoIterator,
    localAttributeOrder: Array[String]
) extends JoinIter {

  private val intersectionAttributes: Array[String] =
    indexIt.localAttributeOrder.intersect(baseIt.localAttributeOrder)

  private val baseAttrAtOutputPos: Array[Int] =
    baseIt.localAttributeOrder.map(localAttributeOrder.indexOf)
  private val indexAttrAtOutputPos: Array[Int] =
    indexIt.localAttributeOrder.map(localAttributeOrder.indexOf)
  private val intersectionAttrAtBaseAttrPos: Array[Int] =
    intersectionAttributes.map(baseIt.localAttributeOrder.indexOf)

  private val indexAttrSize: Int = indexIt.localAttributeOrder.length
  private val baseAttrSize: Int = baseIt.localAttributeOrder.length
  private val intersectionAttrSize: Int = intersectionAttributes.length

  private val outputRow =
    new Array[OldInternalDataType](localAttributeOrder.length)
  private val prefixRow =
    new Array[OldInternalDataType](intersectionAttributes.length)

  private var isIndexItInitialized = false

  private lazy val cache: Cache = {

    if (!SeccoConfiguration.newDefaultConf().enableCache) {
      null
    } else {
      if (indexIt.isInstanceOf[TableIter] || indexIt.isInstanceOf[JoinIter]) {
        null
      } else if (
        baseIt.localAttributeOrder.startsWith(intersectionAttributes)
      ) {
        CacheFactory.genLastUsedCache(
          intersectionAttributes,
          indexIt.localAttributeOrder.diff(intersectionAttributes)
        )
      } else {
        CacheFactory.genLRUCache(
          intersectionAttributes,
          indexIt.localAttributeOrder.diff(intersectionAttributes)
        )
      }
    }

  }

  private var cachedIndexIt: Iterator[OldInternalRow] = _

  override def reset(prefix: OldInternalRow): SeccoIterator = {
    isIndexItInitialized = false
    baseIt.reset(prefix)
    this
  }

  override def hasNext: Boolean = {
    if (isIndexItInitialized && cachedIndexIt.hasNext) {
      return true
    }

    while (baseIt.hasNext) {
      val baseRow = baseIt.next()
      var i = 0
      while (i < baseAttrSize) {
        outputRow(baseAttrAtOutputPos(i)) = baseRow(i)
        i += 1
      }

      var j = 0
      while (j < intersectionAttrSize) {
        prefixRow(j) = baseRow(intersectionAttrAtBaseAttrPos(j))
        j += 1
      }

      if (cache != null) {
        val cached = cache.getOrElse(prefixRow, null)
        if (cached != null) {
          cachedIndexIt = cached
          isIndexItInitialized = true
        } else {
          indexIt.reset(prefixRow)
          isIndexItInitialized = true
          cachedIndexIt = cache.put(prefixRow, indexIt)
        }
      } else {
        indexIt.reset(prefixRow)
        isIndexItInitialized = true
        cachedIndexIt = indexIt
      }

      if (cachedIndexIt.hasNext) {
        return true
      }
    }

    false
  }

  override def next(): OldInternalRow = {
    val indexRow = cachedIndexIt.next()
    var i = intersectionAttrSize
    while (i < indexAttrSize) {
      outputRow(indexAttrAtOutputPos(i)) = indexRow(i)
      i += 1
    }

    outputRow
  }
}
