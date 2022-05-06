//package org.apache.spark.secco.execution.plan.computation.utils
//
//import org.apache.spark.secco.execution.OldInternalRow
//
////TODO: this class should add more checking to ensure its correctness
//class PartialLeapFrogJoin(
//    tries: Array[Trie],
//    attrOrder: Seq[String],
//    schemas: Seq[Seq[String]]
//) extends LeapFrogJoin(tries, attrOrder, schemas) {
//
//  var startPos = 0
//
//  override def init(): LeapFrogJoin = {
//    this
//  }
//
//  //fixme: this method should implemented to ensure the correctness of this class.
//  def contains(prefix: OldInternalRow): Boolean = ???
//
//  var idPrinted = false
//
//  //fixme: this is an unsafe method, PartialLeapFrogJoin will not check if the prefix is consistent with the relations.
//  // this method is correct if the attributes in prefix are disconnected in the relations.
//  //   or
//  // this method is correct if it is used in processing subgraph query using GHD with induced hypernodes.
//  def unsafeInit(prefix: OldInternalRow) = {
//    firstInitialized = false
//    hasEnd = false
//    var i = 0
//    val prefixSize = prefix.size
//    startPos = prefixSize
//    while (i < prefixSize) {
//      binding(i) = prefix(i)
//      i += 1
//    }
//
//    i = startPos
//    while (i < attrSize) {
//      fixIterators(i)
//      i += 1
//    }
//
//    lastIterator = unaryIterators(lastIdx)
//
//    this
//  }
//
//  override protected def fixIterators(idx: Int): Boolean = {
//
//    if (idx == startPos && firstInitialized != true) {
//
//      firstInitialized = true
//      //case: fix the iterator for first attribute, this will be fixed only once
//      val it = constructIthIterator(idx)
//
//      if (it.hasNext) {
//        unaryIterators(idx) = it
//        return true
//      } else {
//        hasEnd = true
//        return false
//      }
//
//    }
//
//    if (idx == startPos && firstInitialized == true) {
//      hasEnd = true
//      return false
//    }
//
//    while (!hasEnd) {
//      //case: fix the iterator for the rest of the attributes
//      val prevIdx = idx - 1
//      val prevIterator = unaryIterators(prevIdx)
//
//      //debug
////      if (prevIterator == null) {
////        println(
////          s"[debug]: prevIdx:${prevIdx}, idx:${idx}, startPos:${startPos}, firstInitialized:${firstInitialized}, hasEnd:${hasEnd}"
////        )
////
////        if (!idPrinted) {
////          idPrinted = true
////          println(this)
////        }
////
////      }
//
//      while (prevIterator.hasNext) {
//        binding(prevIdx) = prevIterator.next()
//        val it = constructIthIterator(idx)
//        if (it.hasNext) {
//          unaryIterators(idx) = it
//          return true
//        }
//      }
//
//      if (prevIdx == startPos) {
//        hasEnd = true
//        return false
//      } else {
//        fixIterators(prevIdx)
//      }
//    }
//
//    return hasEnd
//  }
//
//}
//
//object PartialLeapFrogJoin {
//  def apply(
//      tries: Array[Trie],
//      attrOrder: Seq[String],
//      schemas: Seq[Seq[String]]
//  ): PartialLeapFrogJoin =
//    new PartialLeapFrogJoin(tries, attrOrder, schemas)
//
//}
