package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}

class LeapFrogJoin(
    tries: Array[Trie],
    attrOrder: Seq[String],
    schemas: Seq[Seq[String]]
) extends Iterator[OldInternalRow] {

  lazy val numRelations: Int = schemas.size
  lazy val attrSize: Int = attrOrder.size
  protected val binding = new Array[OldInternalDataType](attrSize)
  protected var hasEnd = false
  protected val unaryIterators =
    new Array[Iterator[OldInternalDataType]](attrSize)
  protected val lastIdx: Int = attrSize - 1
  protected var lastIterator: Iterator[OldInternalDataType] = null
  //init
//  init()

  //record
  // 1) for each attribute: the relevant relations
  // 2)   for each relation: the index of prefix in binding
  //
  //output format:
  // Attributes(Relevant Relations(Relation Schema, Index of Relation Schema, Index of prefix, An temp array for store partial biniding))
  lazy val relevantRelationForAttrMap
      : Array[(Array[(Int, Array[Int], ArraySegment)], Array[ArraySegment])] = {
    Range(0, attrSize).toArray.map { idx =>
      val curAttr = attrOrder(idx)
      val relevantRelation = schemas.zipWithIndex.filter { case (schema, _) =>
        schema.contains(curAttr)
      }

      //set up prefix information for each relation
      val prefixPosForEachRelation = relevantRelation.map {
        case (schema, schemaPos) =>
          val relativeOrder = attrOrder.filter(schema.contains)
          val attrPos = relativeOrder.indexOf(curAttr)
          val partialBindingPos = new Array[Int](attrPos)
          val partialBinding =
            ArraySegment(new Array[OldInternalDataType](attrPos))

          var i = 0
          while (i < attrPos) {
            partialBindingPos(i) = attrOrder.indexOf(relativeOrder(i))
            i += 1
          }

          (schemaPos, partialBindingPos, partialBinding)
      }.toArray

      //set up the ArraySegments
      val segmentArrays = Array.fill(prefixPosForEachRelation.length)(
        new ArraySegment(Array(), 0, 0, 0)
      )

      (prefixPosForEachRelation, segmentArrays)
    }
  }

  //init unary iterator for all attributes
  def init(): LeapFrogJoin = {

    //gradually init the unary iterator for 0, 1, ..., n-th attribute
    var i = 0
    while (i < attrSize) {
      fixIterators(i)
      i += 1
    }
    lastIterator = unaryIterators(lastIdx)

    this
  }

  //construct the unary iterator for the i-th attribute given prefix consisting of 0 to i-1th attribute
  //Noted: this class wouldn't produce empty unary iterator unless all prefix for 0 to i-1th attribute has been tested.
  protected var firstInitialized = false

  protected def fixIterators(idx: Int): Boolean = {

    if (idx == 0 && !firstInitialized) {
      firstInitialized = true
      //case: fix the iterator for first attribute, this will be fixed only once
      val it = constructIthIterator(idx)

      if (it.hasNext) {
        unaryIterators(idx) = it
        return true
      } else {
        hasEnd = true
        return false
      }
    } else if (idx == 0 && firstInitialized) {
      hasEnd = true
      return false
    }

    while (!hasEnd) {
      //case: fix the iterator for the rest of the attributes
      val prevIdx = idx - 1
      val prevIterator = unaryIterators(prevIdx)
      while (prevIterator.hasNext) {
        binding(prevIdx) = prevIterator.next()
        val it = constructIthIterator(idx)
        if (it.hasNext) {
          unaryIterators(idx) = it
          return true
        }
      }

      if (prevIdx == 0) {
        hasEnd = true
        return false
      } else {
        fixIterators(prevIdx)
      }

    }

    return hasEnd

  }

  //construct the unary iterator for the i-th attribute given prefix consisting of 0 to i-1th attribute
  //Noted: this class could return empty unary iterator
  protected def constructIthIterator(
      idx: Int
  ): Iterator[OldInternalDataType] = {

    val (prefixPosForEachRelation, segmentArrays) = relevantRelationForAttrMap(
      idx
    )

    var i = 0
    val sizeOfSegmentArrays = segmentArrays.length
    while (i < sizeOfSegmentArrays) {

      val (triePos, partialBindingPos, curBinding) =
        prefixPosForEachRelation(i)

      var j = 0
      val curBindingSize = partialBindingPos.length
      while (j < curBindingSize) {
        curBinding(j) = binding(partialBindingPos(j))
        j += 1
      }

      tries(triePos).nextLevel(curBinding, segmentArrays(i))
//      tries(triePos).nextLevelWithAdjust(curBinding, segmentArrays(i))
      i += 1
    }

//    new LeapFrogUnaryIterator(segmentArrays)
    IntersectionIterator.leapfrogIt(segmentArrays)
//    new IntersectedListIterator(segmentArrays)
//    IntersectionIterator.listIt(segmentArrays)
  }

  override def hasNext: Boolean = {
    if (!hasEnd) {
      //      lastIterator = unaryIterators(lastIdx)
      //check if last iterator hasNext, if not, trying to produce new last iterator
      if (lastIterator.hasNext) {
        true
      } else {
        fixIterators(lastIdx)
        lastIterator = unaryIterators(lastIdx)
        !hasEnd
      }
    } else {
      !hasEnd
    }

  }

  //return the underlying binding array
  def getBinding(): Array[OldInternalDataType] = {
    binding
  }

  override def next(): Array[OldInternalDataType] = {
    binding(lastIdx) = lastIterator.next()
    binding
  }

//  override def longSize() = {
//
//    var count = 0L
//    while (!hasEnd) {
//      count += unaryIterators(lastIdx).size
//      fixIterators(lastIdx)
//    }
//
//    count
//  }

}

object LeapFrogJoin {
  def apply(
      tries: Array[Trie],
      attrOrder: Seq[String],
      schemas: Seq[Seq[String]]
  ): LeapFrogJoin = {
    val lf = new LeapFrogJoin(tries, attrOrder, schemas)
    lf.init()
  }
}

//def initRelations() = {
//  //init content of relations
//  var i = 0
//  while (i < numRelations) {
//
//  //reorder the content according to attribute order
//  reorderRelations(i)
//
//  //construct trie based on reordered content
//  tries(i) = ArrayTrie(contents(i), schemas(i).arity)
//  i += 1
//}
//}
//
//  //reorder tuples of relation-idx according to attribute order
//  protected def reorderRelations(idx: Int): Unit = {
//  val content = contents(idx)
//  val schema = schemas(idx)
//
//  //The func that mapping idx-th value of each tuple to j-th pos, where j-th position is the reletive order of idx-th attribute determined via attribute order
//  val tupleMappingFunc =
//  attrOrders.filter(schema.containAttribute).map(schema.attrIDs.indexOf(_))
//
//  val contentArity = schema.arity
//  val contentSize = content.size
//  val tempArray = new Array[DataType](contentArity)
//
//  var i = 0
//  while (i < contentSize) {
//  val tuple = content(i)
//  var j = 0
//
//  while (j < contentArity) {
//  tempArray(j) = tuple(j)
//  j += 1
//}
//
//  j = 0
//  while (j < contentArity) {
//  tuple(j) = tempArray(tupleMappingFunc(j))
//  j += 1
//}
//
//  i += 1
//}
//}

//abstract class LongSizeIterator[T] extends Iterator[T] {
//  def longSize(): Long
//  def recordCardinalityAndTime(): (Double, Double) = {
//
//    val time1 = System.nanoTime()
//    val cardinality = longSize().toDouble
//    val time2 = System.nanoTime()
//    val time = ((time2 - time1).toDouble) / Math.pow(10, 6)
//
//    (cardinality, time)
//
//  }
//}
