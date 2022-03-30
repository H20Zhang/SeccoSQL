package org.apache.spark.secco.execution

import org.apache.spark.secco.errors.QueryExecutionErrors
import org.apache.spark.secco.execution.storage.row.{InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.expression.codegen.GenerateUnsafeProjection

import scala.+:
import scala.collection.mutable


/**
  * Interface for a hashed relation by some key. Use [[HashedRelation.apply]] to create a concrete
  * object.
  */
private[execution] sealed trait HashedRelation{
  /**
    * Returns matched rows.
    *
    * Returns null if there is no matched rows.
    */
  def get(key: InternalRow): Iterator[InternalRow]

  /**
    * Returns matched rows for a key that has only one column with LongType.
    *
    * Returns null if there is no matched rows.
    */
  def get(key: Long): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns the matched single row.
    */
  def getValue(key: InternalRow): InternalRow

  /**
    * Returns the matched single row with key that have only one column of LongType.
    */
  def getValue(key: Long): InternalRow = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns an iterator for key index and matched rows.
    *
    * Returns null if there is no matched rows.
    */
  def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns key index and matched single row.
    * This is for unique key case.
    *
    * Returns null if there is no matched rows.
    */
  def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns an iterator for keys index and rows of InternalRow type.
    */
  def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns the maximum number of allowed keys index.
    */
  def maxNumKeysIndex: Int = {
    throw new UnsupportedOperationException
  }

  /**
    * Returns true iff all the keys are unique.
    */
  def keyIsUnique: Boolean

  /**
    * Returns an iterator for keys of InternalRow type.
    */
  def keys(): Iterator[InternalRow]

  /**
    * Returns a read-only copy of this, to be safely used in current thread.
    */
  def asReadOnlyCopy(): HashedRelation

  /**
    * Release any used resources.
    */
  def close(): Unit
}

private[execution] object HashedRelation {

  /**
    * Create a HashedRelation from an Iterator of InternalRow.
    *
    * @param allowsNullKey        Allow NULL keys in HashedRelation.
    *                             This is used for full outer join in `ShuffledHashJoinExec` only.
    * @param ignoresDuplicatedKey Ignore rows with duplicated keys in HashedRelation.
    *                             This is only used for semi and anti join without join condition in
    *                             `ShuffledHashJoinExec` only.
    */
  def apply(
             input: Iterator[InternalRow],
             key: Seq[Expression],
             sizeEstimate: Int = 64,
             isNullAware: Boolean = false,
             allowsNullKey: Boolean = false,
             ignoresDuplicatedKey: Boolean = false): HashedRelation = {

    if (!input.hasNext && !allowsNullKey) {
      EmptyHashedRelation
    } else {
      ScalaMapHashedRelation(input, key, sizeEstimate, isNullAware, allowsNullKey,
        ignoresDuplicatedKey)
    }
  }
}

/**
  * A wrapper for key index and value in InternalRow type.
  * Designed to be instantiated once per thread and reused.
  */
private[execution] class ValueRowWithKeyIndex {
  private var keyIndex: Int = _
  private var value: InternalRow = _

  /** Updates this ValueRowWithKeyIndex by updating its key index.  Returns itself. */
  def withNewKeyIndex(newKeyIndex: Int): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    this
  }

  /** Updates this ValueRowWithKeyIndex by updating its value.  Returns itself. */
  def withNewValue(newValue: InternalRow): ValueRowWithKeyIndex = {
    value = newValue
    this
  }

  /** Updates this ValueRowWithKeyIndex.  Returns itself. */
  def update(newKeyIndex: Int, newValue: InternalRow): ValueRowWithKeyIndex = {
    keyIndex = newKeyIndex
    value = newValue
    this
  }

  def getKeyIndex: Int = {
    keyIndex
  }

  def getValue: InternalRow = {
    value
  }
}


/**
  * A special HashedRelation indicating that it's built from a empty input:Iterator[InternalRow].
  * get & getValue will return null just like
  * empty LongHashedRelation or empty ScalaMapHashedRelation does.
  */
case object EmptyHashedRelation extends HashedRelation {
  override def get(key: Long): Iterator[InternalRow] = null

  override def get(key: InternalRow): Iterator[InternalRow] = null

  override def getValue(key: Long): InternalRow = null

  override def getValue(key: InternalRow): InternalRow = null

  override def asReadOnlyCopy(): EmptyHashedRelation.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    Iterator.empty
  }

  override def close(): Unit = {}
}

/**
  * A special HashedRelation indicating that it's built from a non-empty input:Iterator[InternalRow]
  * with all the keys to be null.
  */
case object HashedRelationWithAllNullKeys extends HashedRelation {
  override def get(key: InternalRow): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def getValue(key: InternalRow): InternalRow = {
    throw new UnsupportedOperationException
  }

  override def asReadOnlyCopy(): HashedRelationWithAllNullKeys.type = this

  override def keyIsUnique: Boolean = true

  override def keys(): Iterator[InternalRow] = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {}
}

/**
  * A HashedRelation for UnsafeRow, which is backed BytesToBytesMap.
  *
  * It's serialized in the following format:
  *  [number of keys]
  *  [size of key] [size of value] [key bytes] [bytes for value]
  */
private class ScalaMapHashedRelation(
                                           private var numKeys: Int,
                                           private var numFields: Int,
                                           private var hashMap: mutable.Map[InternalRow, List[InternalRow]])
  extends HashedRelation {

  override def toString: String = {
    super.toString + " | " + hashMap
  }

  private def this() = this(0, 0, null)  // Needed for serialization

//  override def keyIsUnique: Boolean = hashMap.keys.size == hashMap.values.flatMap(_.toSeq).size
  override def keyIsUnique: Boolean = hashMap.values.forall(_.length == 1)

  override def asReadOnlyCopy(): ScalaMapHashedRelation = {
    new ScalaMapHashedRelation(numKeys, numFields, hashMap)
  }

//  override def estimatedSize: Long = binaryMap.getTotalMemoryConsumption

  // re-used in get()/getValue()/getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  var resultRow = new UnsafeInternalRow(numFields)

  // re-used in getWithKeyIndex()/getValueWithKeyIndex()/valuesWithKeyIndex()
  val valueRowWithKeyIndex = new ValueRowWithKeyIndex

  override def get(key: InternalRow): Iterator[InternalRow] = {
    val unsafeKey = key.asInstanceOf[UnsafeInternalRow]
    val optRowSeq = hashMap.get(unsafeKey)
    if (optRowSeq.isDefined)
      optRowSeq.get.toIterator
    else
      null
  }

  def getValue(key: InternalRow): InternalRow = {
    val unsafeKey = key.asInstanceOf[UnsafeInternalRow]
    val resultRowSeq = hashMap.getOrElse(unsafeKey, null)
    if(resultRowSeq != null && resultRowSeq.nonEmpty)
      resultRowSeq.head
    else
      null
  }

  override def getWithKeyIndex(key: InternalRow): Iterator[ValueRowWithKeyIndex] = {
    val unsafeKey = key.asInstanceOf[UnsafeInternalRow]
    val resultRowSeq = hashMap.getOrElse(unsafeKey, null)
    if(resultRowSeq != null && resultRowSeq.nonEmpty)
      resultRowSeq.map(valueRowWithKeyIndex.update(hashMap.keys.toSeq.indexOf(unsafeKey), _)).toIterator
    else
      null
  }

  override def getValueWithKeyIndex(key: InternalRow): ValueRowWithKeyIndex = {
    val unsafeKey = key.asInstanceOf[UnsafeInternalRow]
    val resultRowSeq = hashMap.getOrElse(unsafeKey, null)
    if(resultRowSeq != null && resultRowSeq.nonEmpty)
      valueRowWithKeyIndex.update(hashMap.keys.toSeq.indexOf(unsafeKey), resultRowSeq.head)
    else
      null
  }

  override def valuesWithKeyIndex(): Iterator[ValueRowWithKeyIndex] = {
    val iter = hashMap.zipWithIndex.flatMap{
      case ((rowKey, rowSeqValue), index) => rowSeqValue.map((index, _))}.toIterator

    new Iterator[ValueRowWithKeyIndex] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): ValueRowWithKeyIndex = {
        if (!hasNext) {
          throw QueryExecutionErrors.endOfIteratorError()
        }
        val indexAndRow = iter.next()
//        resultRow.pointTo(loc.getValueBase, loc.getValueOffset, loc.getValueLength)
        valueRowWithKeyIndex.update(indexAndRow._1, indexAndRow._2)
      }
    }
  }

  override def maxNumKeysIndex: Int = {
    hashMap.size
  }

  override def keys(): Iterator[InternalRow] = hashMap.keys.toIterator

  override def close(): Unit = {}
}

private object ScalaMapHashedRelation {

  def apply(
             input: Iterator[InternalRow],
             key: Seq[Expression],  // lgh: Bound keys
             sizeEstimate: Int,
             isNullAware: Boolean = false,
             allowsNullKey: Boolean = false,
             ignoresDuplicatedKey: Boolean = false): HashedRelation = {
    require(!(isNullAware && allowsNullKey),
      "isNullAware and allowsNullKey cannot be enabled at same time")

//    val pageSizeBytes = Option(SparkEnv.get).map(_.memoryManager.pageSizeBytes)
//      .getOrElse(new SparkConf().get(BUFFER_PAGESIZE).getOrElse(16L * 1024 * 1024))
    val binaryMap: mutable.Map[InternalRow, List[InternalRow]] = new mutable.HashMap[InternalRow, List[InternalRow]]()

    // Create a mapping of buildKeys -> rows
    val keyGenerator = GenerateUnsafeProjection.generate(key)  // edited by lgh
    //    val keyGenerator = UnsafeProjection.create(key)
    var numFields = 0
    while (input.hasNext) {
      val row = input.next().asInstanceOf[UnsafeInternalRow]
      numFields = row.numFields
      val key = keyGenerator(row)
      if (!key.anyNull || allowsNullKey) {
        val loc = binaryMap.get(key)
        if (!(ignoresDuplicatedKey && loc.isDefined)) {
          val curValue = binaryMap.get(key)
          if(curValue.isDefined){
            binaryMap(key.copy()) = row.copy() :: binaryMap(key)
          } else {
            binaryMap += (key.copy() -> List(row.copy()))
          }
        }
      } else if (isNullAware) {
        binaryMap.clear()
        return HashedRelationWithAllNullKeys
      }
    }

    new ScalaMapHashedRelation(key.size, numFields, binaryMap)
  }
}