package org.apache.spark.secco.execution.plan.computation.utils

import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.execution.{InternalDataType, InternalRow}
import org.apache.spark.secco.execution.plan.computation.iter.{
  EmptyIterator,
  SingularIterator
}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object CacheFactory {
  def genLRUCache(
      keyAttributes: Array[String],
      valueAttributes: Array[String]
  ): Cache = {

    if (valueAttributes.nonEmpty) {
      new LRUMapCache(keyAttributes, valueAttributes)
    } else {
      new LRUSetCache(keyAttributes)
    }
  }

  def genLastUsedCache(
      keyAttributes: Array[String],
      valueAttributes: Array[String]
  ) = {

    if (valueAttributes.nonEmpty) {
      new LastUsedMapCache(keyAttributes, valueAttributes)
    } else {
      new LastUsedSetCache(keyAttributes)
    }

  }

}

trait Cache {

  /** store InternalRows consecutively */
  case class ConsecutiveValueArray(
      arity: Int,
      underlyingArray: Array[InternalDataType]
  ) {}

  object ConsecutiveValueArray {
    def apply(
        iterator: Iterator[InternalRow]
    ): ConsecutiveValueArray = {
      val buffer = ArrayBuffer[InternalDataType]()
      while (iterator.hasNext) {
        var i = 0
        val row = iterator.next()
        while (i < valueSize) {
          buffer += row(valuePos(i))
          i += 1
        }
      }

      var i = 0
      val bufferSize = buffer.size
      val underlyingArray = new Array[InternalDataType](bufferSize)
      while (i < bufferSize) {
        underlyingArray(i) = buffer(i)
        i += 1
      }

      new ConsecutiveValueArray(valueSize, underlyingArray)
    }
  }

  /** output consecutive stored InternalRows */
  class ConsecutiveKeyValueRowIterator extends Iterator[InternalRow] {

    private val _outputRow = new Array[InternalDataType](attributes.length)
    private var _curPos = 0
    private var _underlyingArraySize = 0
    private var _underlyingArray: Array[InternalDataType] = _

    def reset(
        key: Array[InternalDataType],
        consecutiveArray: ConsecutiveValueArray
    ): ConsecutiveKeyValueRowIterator = {
      _curPos = 0
      _underlyingArray = consecutiveArray.underlyingArray
      _underlyingArraySize = _underlyingArray.length
      var i = 0
      while (i < keySize) {
        _outputRow(i) = key(i)
        i += 1
      }
      this
    }

    override def hasNext: Boolean = _curPos < _underlyingArraySize
    override def next(): InternalRow = {
      var i = 0
      while (i < valueSize) {
        _outputRow(valuePos(i)) = _underlyingArray(_curPos + i)
        i += 1
      }

      _curPos += valueSize

      _outputRow
    }
  }

  val attributes: Array[String] = keyAttributes ++ valueAttributes
  val keyPos: Array[Int] = keyAttributes.map(attributes.indexOf)
  val valuePos: Array[Int] = valueAttributes.map(attributes.indexOf)

  val keySize: Int = keyAttributes.length
  val valueSize: Int = valueAttributes.length

  def keyAttributes: Array[String]
  def valueAttributes: Array[String]

  def contains(key: Array[InternalDataType]): Boolean
  def get(key: Array[InternalDataType]): Iterator[InternalRow]
  def getOrElse(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow]
  def put(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow]
}

class LRUMapCache(
    val keyAttributes: Array[String],
    val valueAttributes: Array[String]
) extends Cache {

  assert(valueAttributes.nonEmpty)

  val lruKeyArray = new Array[InternalDataType](keySize)
  val lruKey: mutable.WrappedArray[InternalDataType] =
    mutable.WrappedArray.make[InternalDataType](
      lruKeyArray
    )
  val map =
    new LRUHashMap[mutable.WrappedArray[
      InternalDataType
    ], ConsecutiveValueArray](
      SeccoConfiguration.newDefaultConf().cacheSize
    )

  val iterator = new ConsecutiveKeyValueRowIterator()
  val emptyIterator = new EmptyIterator

  @inline private def copyToKey(key: Array[InternalDataType]): Unit = {
    var i = 0
    while (i < keySize) {
      lruKeyArray(i) = key(i)
      i += 1
    }
  }

  @inline private def newLRUKeyInstance(
      key: Array[InternalDataType]
  ): mutable.WrappedArray[InternalDataType] = {
    val lruKeyArray = new Array[InternalDataType](keySize)
    val lruKey = mutable.WrappedArray.make[InternalDataType](
      lruKeyArray
    )
    var i = 0
    while (i < keySize) {
      lruKeyArray(i) = key(i)
      i += 1
    }
    lruKey
  }

  override def contains(key: Array[InternalDataType]): Boolean = {
    copyToKey(key)
    map.contain(lruKey)
  }

  override def get(key: Array[InternalDataType]): Iterator[InternalRow] = {
    getOrElse(key, emptyIterator)
  }

  override def put(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    val newLRUKey = newLRUKeyInstance(key)
    val lruValue = ConsecutiveValueArray(it)
    map.put(newLRUKey, lruValue)
    iterator.reset(key, lruValue)
  }

  override def getOrElse(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    copyToKey(key)
    val res = map.getOrDefault(key, null)
    if (res != null) {
      iterator.reset(key, res)
    } else {
      it
    }
  }
}

class LastUsedMapCache(
    val keyAttributes: Array[String],
    val valueAttributes: Array[String]
) extends Cache {

  assert(valueAttributes.nonEmpty)

  private val lastKey: Array[InternalDataType] =
    new Array[InternalDataType](keySize) map (f => Double.MaxValue)

  private var lastConsecutiveArray: ConsecutiveValueArray = _
  private val iterator: ConsecutiveKeyValueRowIterator =
    new ConsecutiveKeyValueRowIterator
  private val emptyIterator = new EmptyIterator

  override def contains(key: Array[InternalDataType]): Boolean = {
    var i = 0
    while (i < keySize) {
      if (key(i) != lastKey(i)) {
        return false
      }
      i += 1
    }

    true
  }

  override def get(key: Array[InternalDataType]): Iterator[InternalRow] = {
    getOrElse(key, emptyIterator)
  }

  override def getOrElse(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    if (contains(key)) {
      iterator.reset(lastKey, lastConsecutiveArray)
    } else {
      it
    }
  }

  override def put(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    var i = 0
    while (i < keySize) {
      lastKey(i) = key(i)
      i += 1
    }
    lastConsecutiveArray = ConsecutiveValueArray(it)
    iterator.reset(lastKey, lastConsecutiveArray)
  }
}

class LRUSetCache(
    val keyAttributes: Array[String],
    val valueAttributes: Array[String] = Array.empty
) extends Cache {
  val lruKeyArray = new Array[InternalDataType](keySize)
  val lruKey: mutable.WrappedArray[InternalDataType] =
    mutable.WrappedArray.make[InternalDataType](
      lruKeyArray
    )
  val map =
    new LRUHashMap[mutable.WrappedArray[InternalDataType], Boolean](
      SeccoConfiguration.newDefaultConf().cacheSize
    )

  val iterator = new SingularIterator()
  val emptyIterator = new EmptyIterator

  @inline private def copyToKey(key: Array[InternalDataType]): Unit = {
    var i = 0
    while (i < keySize) {
      lruKeyArray(i) = key(i)
      i += 1
    }
  }

  @inline private def newLRUKeyInstance(
      key: Array[InternalDataType]
  ): mutable.WrappedArray[InternalDataType] = {
    val lruKeyArray = new Array[InternalDataType](keySize)
    val lruKey = mutable.WrappedArray.make[InternalDataType](
      lruKeyArray
    )
    var i = 0
    while (i < keySize) {
      lruKeyArray(i) = key(i)
      i += 1
    }
    lruKey
  }

  override def contains(key: Array[InternalDataType]): Boolean = {
    copyToKey(key)
    map.contain(lruKey)
  }

  override def get(key: Array[InternalDataType]): Iterator[InternalRow] = {
    getOrElse(key, emptyIterator)
  }

  override def put(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    val newLRUKey = newLRUKeyInstance(key)
    map.put(newLRUKey, true)
    iterator.reset(key)
  }

  override def getOrElse(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    copyToKey(key)
    val res = map.getOrDefault(key, false)
    if (res != false) {
      iterator.reset(key)
    } else {
      it
    }
  }
}

class LastUsedSetCache(
    val keyAttributes: Array[String],
    val valueAttributes: Array[String] = Array.empty
) extends Cache {

  private val lastKey: Array[InternalDataType] =
    new Array[InternalDataType](keySize) map (f => Double.MaxValue)

  private val iterator: SingularIterator = new SingularIterator
  private val emptyIterator = new EmptyIterator

  override def contains(key: Array[InternalDataType]): Boolean = {
    var i = 0
    while (i < keySize) {
      if (key(i) != lastKey(i)) {
        return false
      }
      i += 1
    }

    true
  }

  override def get(key: Array[InternalDataType]): Iterator[InternalRow] = {
    getOrElse(key, emptyIterator)
  }

  override def getOrElse(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    if (contains(key)) {
      iterator.reset(lastKey)
    } else {
      it
    }
  }

  override def put(
      key: Array[InternalDataType],
      it: Iterator[InternalRow]
  ): Iterator[InternalRow] = {
    var i = 0
    while (i < keySize) {
      lastKey(i) = key(i)
      i += 1
    }
    iterator.reset(lastKey)
  }
}
