package org.apache.spark.secco.execution.plan.computation.iter

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}
import org.apache.spark.secco.execution.plan.computation.utils.{
  BufferPool,
  InternalRowBufferPoolImpl,
  LexicalOrderComparator
//  WrappedArrayBufferPoolImpl
}
import org.apache.spark.secco.execution.plan.support.FuncGenSupport
import org.apache.spark.secco.types.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait SemiringAggregateIter extends SeccoIterator with FuncGenSupport {
  def childIter: SeccoIterator
  def groupingList: Seq[String]
  def semiringList: (String, String)

  lazy val multiplyFunc: OldInternalRow => OldInternalDataType =
    genMultiplyFunc(semiringList._2, childIter.localAttributeOrder)
  lazy val sumFunc
      : (OldInternalDataType, OldInternalDataType) => OldInternalDataType =
    genSumFunc(
      semiringList._1
    )
  lazy val (groupProjectionFunc, groupProjectedRow) = {
    genProjectionFunc(groupingList, childIter.localAttributeOrder)
  }

  override def result(): Array[OldInternalRow] = {

    val localAttributeSize = localAttributeOrder.length

    val lexicalOrderComparator = new LexicalOrderComparator(
      localAttributeSize
    )

    val groupKeyArray = new Array[OldInternalDataType](localAttributeSize)
    val groupKey = mutable.WrappedArray.make[OldInternalDataType](groupKeyArray)

    val aggregateGroupMap =
      mutable
        .HashMap[mutable.WrappedArray[
          OldInternalDataType
        ], OldInternalDataType]()

    childIter.foreach { row =>
      groupProjectionFunc(row)
      var i = 0
      while (i < localAttributeSize - 1) {
        groupKeyArray(i) = groupProjectedRow(i)
        i += 1
      }

      if (aggregateGroupMap.contains(groupKey)) {
        aggregateGroupMap(groupKey) =
          sumFunc(aggregateGroupMap(groupKey), multiplyFunc(row))
      } else {
        aggregateGroupMap(groupKey.clone()) = multiplyFunc(row)
      }
    }

    val aggregateGroupArray = new Array[OldInternalRow](aggregateGroupMap.size)
    var i = 0
    aggregateGroupMap.foreach { projectedRow =>
      projectedRow._1(localAttributeSize - 1) = projectedRow._2
      aggregateGroupArray(i) = projectedRow._1.array
      i += 1
    }

    java.util.Arrays
      .sort(aggregateGroupArray, lexicalOrderComparator)

    aggregateGroupArray
  }
}

case class MatchedPrefixSemiringAggregateIter(
    childIter: SeccoIterator,
    groupingList: Seq[String],
    semiringList: (String, String),
    localAttributeOrder: Array[String]
) extends SemiringAggregateIter {
  override def reset(prefix: OldInternalRow): SeccoIterator = {
    nextDefined = false
    outputDefined = false
    isInitialized = false
    childIter.reset(prefix)
    this
  }

  val groupArity: Int = groupingList.length
  val nextGroupKey = new Array[OldInternalDataType](groupArity)
  var nextDefined = false
  var outputDefined = false
  var isInitialized = false
  var aggValue: Double = _
  var outputRow: Array[OldInternalDataType] =
    new Array[OldInternalDataType](localAttributeOrder.length)

  //update nextGroupKey based on row
  private def updateNextGroupKeyAndAggValue(row: OldInternalRow): Unit = {
    //get group projected row
    groupProjectionFunc(row)

    var i = 0
    while (i < groupingList.size) {
      nextGroupKey(i) = groupProjectedRow(i)
      i += 1
    }

    aggValue = multiplyFunc(row)

    nextDefined = true
  }

  private def updateOutputRow(): Unit = {
    var i = 0
    while (i < groupingList.size) {
      outputRow(i) = nextGroupKey(i)
      i += 1
    }

    outputRow(i) = aggValue

    outputDefined = true
    nextDefined = false
  }

  //check if a new group key is encountered.
  private def checkGroupBoundary(): Boolean = {
    var newGroupEncountered = false
    var i = 0
    while (i < groupArity && !newGroupEncountered) {
      if (groupProjectedRow(i) != nextGroupKey(i)) {
        newGroupEncountered = true
      }
      i += 1
    }
    newGroupEncountered
  }

  override def hasNext: Boolean = {

    if (outputDefined) {
      return true
    }

    while (childIter.hasNext) {
      val row = childIter.next()
      groupProjectionFunc(row)

      if (!isInitialized) {
        updateNextGroupKeyAndAggValue(row)
        isInitialized = true
      } else {
        val isBoundaryEncountered = checkGroupBoundary()
        if (isBoundaryEncountered) {
          updateOutputRow()
          updateNextGroupKeyAndAggValue(row)
          return outputDefined
        } else {
          aggValue = sumFunc(aggValue, multiplyFunc(row))
        }
      }
    }

    if (nextDefined) {
      updateOutputRow()
    }

    outputDefined
  }

  override def next(): OldInternalRow = {
    outputDefined = false
    outputRow
  }

  override def result(): Array[OldInternalRow] = {
    val buffer = ArrayBuffer[OldInternalRow]()
    while (hasNext) {
      buffer += next().clone()
    }
    buffer.toArray
  }
}

//TODO: needs to make up a test-case to test this class
case class PartiallyMatchedPrefixSemiringAggregateIter(
    childIter: SeccoIterator,
    groupingList: Seq[String],
    semiringList: (String, String),
    localAttributeOrder: Array[String]
) extends SemiringAggregateIter {

  private val emptyIt = new EmptyIterator

  override def reset(prefix: OldInternalRow): SeccoIterator = {
//    inputBufferPool.reset()
//    outputBufferPool.reset()
    childIter.reset(prefix)
    nextsubGroupKeyDefined = false
    nextsubGroup.clear()
    nextAggregateIter = emptyIt
    this
  }

  lazy val divergePos: Int = {
    var i = 0
    var hasEnd = false
    val size = groupingList.size
    //where groupingList and child.attributeOrder diverage
    var pos = 0
    while (i < size && !hasEnd) {
      if (groupingList(i) != childIter.localAttributeOrder(i)) {
        pos = i
        hasEnd = true
      }
      i += 1
    }
    pos
  }

  val subgroupList: Array[String] = groupingList.slice(0, divergePos).toArray
  val localAttributeSize: Int = localAttributeOrder.length

  val (subgroupProjectionFunc, subgroupProjectedRow) = {
    genProjectionFunc(subgroupList, childIter.localAttributeOrder)
  }

  val subgroupArity: Int = subgroupList.length
  var nextsubGroup: ArrayBuffer[OldInternalRow] = ArrayBuffer()
  val nextsubGroupKey = new Array[OldInternalDataType](subgroupArity)
  var nextsubGroupKeyDefined = false
  var nextAggregateIter: Iterator[OldInternalRow] = Iterator.empty
//  val outputBufferPool: BufferPool[mutable.WrappedArray[InternalDataType]] =
//    new WrappedArrayBufferPoolImpl(localAttributeSize)
//  val inputBufferPool: BufferPool[InternalRow] =
//    new InternalRowBufferPoolImpl(childIter.localAttributeOrder.length)

  //update nextGroupKey based on row
  private def updateNextGroupKey(row: OldInternalRow): Unit = {

//    inputBufferPool.reset()
    //get group projected row
    subgroupProjectionFunc(row)

    var i = 0
    while (i < subgroupArity) {
      nextsubGroupKey(i) = subgroupProjectedRow(i)
      i += 1
    }

    //add row to group
    nextsubGroup.clear()
//    nextsubGroup += inputBufferPool.newInstanceWithArray(row)
    nextsubGroup += row.clone()
    nextsubGroupKeyDefined = true
  }

  //check if a new group key is encountered.
  private def checkGroupBoundary(): Boolean = {
    var newGroupEncountered = false
    var i = 0
    while (i < subgroupArity && !newGroupEncountered) {
      if (subgroupProjectedRow(i) != nextsubGroupKey(i)) {
        newGroupEncountered = true
      }
      i += 1
    }
    newGroupEncountered
  }

  val lexicalOrderComparator = new LexicalOrderComparator(
    localAttributeSize
  )

  //make nextProjectedIter using nextGroup
  private def makeNextAggregateIter() = {

    val aggregateGroupMap =
      mutable
        .HashMap[mutable.WrappedArray[
          OldInternalDataType
        ], OldInternalDataType]()
    nextsubGroup.foreach { row =>
      groupProjectionFunc(row)
      val arr = new Array[OldInternalDataType](localAttributeSize)
      val wrappedArr = mutable.WrappedArray.make[OldInternalDataType](arr)
//      val wrappedArr = outputBufferPool.newInstance()

      var i = 0
      while (i < localAttributeSize - 1) {
        wrappedArr(i) = groupProjectedRow(i)
        i += 1
      }

      if (aggregateGroupMap.contains(wrappedArr)) {
        aggregateGroupMap(wrappedArr) =
          sumFunc(aggregateGroupMap(wrappedArr), multiplyFunc(row))
      } else {
        aggregateGroupMap(wrappedArr) = multiplyFunc(row)
      }
    }

//    val aggregateGroupArrayBuffer = ArrayBuffer[InternalRow]()
//    aggregateGroupMap.foreach { projectedRow =>
//      projectedRow._1(localAttributeSize - 1) = projectedRow._2
//      aggregateGroupArrayBuffer += projectedRow._1.array
//    }
//
//    val aggregateGroupArray = aggregateGroupArrayBuffer.toArray

    val aggregateGroupArray = new Array[OldInternalRow](aggregateGroupMap.size)
    var i = 0
    aggregateGroupMap.foreach { projectedRow =>
      projectedRow._1(localAttributeSize - 1) = projectedRow._2
      aggregateGroupArray(i) = projectedRow._1.array
      i += 1
    }

    java.util.Arrays
      .sort(aggregateGroupArray, lexicalOrderComparator)

    nextsubGroup.clear()

    aggregateGroupArray.iterator
  }

  override def hasNext: Boolean = {

    if (nextAggregateIter.hasNext) {
      true
    } else {
      while (childIter.hasNext) {
        val row = childIter.next()
        if (!nextsubGroupKeyDefined) {
          updateNextGroupKey(row)
        } else {
          //get group projected row
          subgroupProjectionFunc(row)

          //check the boundary of current group
          val newGroupEncountered = checkGroupBoundary()

          if (newGroupEncountered) {
            //make nextAggregateIter
            nextAggregateIter = makeNextAggregateIter()

            //update nextGroupKey
            updateNextGroupKey(row)

            return true
          } else {
//            nextsubGroup += inputBufferPool.newInstanceWithArray(row)
            nextsubGroup += row.clone()
          }
        }
      }

      //handle the case of the last group in iterator.
      if (nextsubGroup.nonEmpty) {
        nextAggregateIter = makeNextAggregateIter()
      }

      nextAggregateIter.hasNext
    }
  }

  override def next(): OldInternalRow = {
    nextAggregateIter.next()
  }
}

case class NoMatchedPrefixSemiringAggregateIter(
    childIter: SeccoIterator,
    groupingList: Seq[String],
    semiringList: (String, String),
    localAttributeOrder: Array[String]
) extends SemiringAggregateIter {
  override def reset(prefix: OldInternalRow): SeccoIterator = ???

  override def hasNext: Boolean = ???

  override def next(): OldInternalRow = ???
}
