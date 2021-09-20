package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.execution.{OldInternalDataType, OldInternalRow}

import scala.collection.mutable

//trait InternalRowHashMap {
//  def isEmpty(): Boolean
//  def get(key: InternalRow): Array[InternalRow]
//}

class InternalRowHashMap(
    keyAttr: Array[String],
    localAttributeOrder: Array[String],
    map: mutable.HashMap[mutable.WrappedArray[OldInternalDataType], Array[
      OldInternalRow
    ]]
) {

  private val keySize = keyAttr.size
  private val keyArray = new Array[OldInternalDataType](keySize)
  private val theKey: mutable.WrappedArray[OldInternalDataType] =
    mutable.WrappedArray.make[OldInternalDataType](
      keyArray
    )

  private val emptyArray = new Array[OldInternalRow](0)

  @inline private def copyToKey(key: Array[OldInternalDataType]): Unit = {
    var i = 0
    while (i < keySize) {
      keyArray(i) = key(i)
      i += 1
    }
  }

  @inline private def newKeyInstance(
      key: Array[OldInternalDataType]
  ): mutable.WrappedArray[OldInternalDataType] = {
    val keyArray = new Array[OldInternalDataType](keySize)
    val theKey = mutable.WrappedArray.make[OldInternalDataType](
      keyArray
    )
    var i = 0
    while (i < keySize) {
      keyArray(i) = key(i)
      i += 1
    }
    theKey
  }

  def contains(key: Array[OldInternalDataType]): Boolean = {
    copyToKey(key)
    map.contains(theKey)
  }

  def getOrElse(
      key: Array[OldInternalDataType],
      default: Array[OldInternalRow]
  ): Array[OldInternalRow] = {
    copyToKey(key)
    map.getOrElse(theKey, default)
  }
  def isEmpty(): Boolean = map.isEmpty
  def get(key: OldInternalRow): Array[OldInternalRow] = {
    getOrElse(key, emptyArray)
  }
  def put(key: OldInternalRow, value: Array[OldInternalRow]): Unit = {
    val newKey = newKeyInstance(key)
    map.put(newKey, value)
  }
}

object InternalRowHashMap {
  def apply(
      keyAttr: Array[String],
      localAttributeOrder: Array[String],
      content: Array[OldInternalRow]
  ): InternalRowHashMap = {
    assert(localAttributeOrder.startsWith(keyAttr))

    //construct map
    val mapBuffer = mutable.HashMap[mutable.WrappedArray[
      OldInternalDataType
    ], mutable.ArrayBuffer[OldInternalRow]]()
    val keySize = keyAttr.size
    val keyArray = new Array[OldInternalDataType](keySize)
    val key: mutable.WrappedArray[OldInternalDataType] =
      mutable.WrappedArray.make[OldInternalDataType](
        keyArray
      )

    var i = 0
    val contentSize = content.size
    while (i < contentSize) {
      val row = content(i)
      var j = 0
      while (j < keySize) {
        keyArray(j) = row(j)
        j += 1
      }

      val buffer = mapBuffer.getOrElse(key, mutable.ArrayBuffer())
      if (buffer.isEmpty) {
        val newKeyArray = new Array[OldInternalDataType](keySize)
        val newKey: mutable.WrappedArray[OldInternalDataType] =
          mutable.WrappedArray.make[OldInternalDataType](
            newKeyArray
          )
        var j = 0
        while (j < keySize) {
          newKeyArray(j) = row(j)
          j += 1
        }

        buffer += row
        mapBuffer(newKey) = buffer
      } else {
        buffer += row
      }

      i += 1
    }

//    val map =
//      new Object2ObjectOpenHashMap[mutable.WrappedArray[DataType], Array[
//        InternalRow
//      ]]()
//    mapBuffer.foreach {
//      case (key, value) =>
//        map.put(key, value.toArray)
//    }

    val map = mapBuffer.map(f => (f._1, f._2.toArray))

    //construct InternalRowHashMap
    new InternalRowHashMap(keyAttr, localAttributeOrder, map)
  }
}
