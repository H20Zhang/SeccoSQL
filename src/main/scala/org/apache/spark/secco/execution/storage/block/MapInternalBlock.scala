package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.row.{GenericInternalRow, InternalRow, UnsafeInternalRow}
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.expression.codegen.GenerateUnsafeProjection
import org.apache.spark.secco.types.StructType
import org.apache.spark.secco.util.misc.LogAble

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

class HashMapInternalBlock(private val hashMap: HashMap[InternalRow, Array[InternalRow]],
                           val blockSchema: Array[Attribute],
                           val keySchema: Array[Attribute])
  extends InternalBlock with MapLike {

  override def iterator: Iterator[InternalRow] = hashMap.values.flatten.toIterator

  private lazy val numRow = rowArray.length
  /** Return the numbers of rows of [[InternalBlock]] */
  override def size(): Long = numRow

  override def isEmpty(): Boolean = hashMap.isEmpty

  override def nonEmpty(): Boolean = hashMap.nonEmpty

  override def schema(): StructType = StructType.fromAttributes(blockSchema)

  /** Sort the rows by an dictionary order.
    *
    * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
    * @return a new sorted InternalBlock.
    */
  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock =
    throw new Exception("sort is not allowed in HashMapInternalBlock")

  override def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean
  ): InternalBlock = {
    if (maintainSortOrder)
      throw new Exception("sort is not allowed in HashMapInternalBlock")
    if (other.isEmpty()) {
      return this
    }
    val tempHashMap = mutable.HashMap(hashMap.toSeq:_*)
    other match {
      case otherHashMapBlock: HashMapInternalBlock =>
        val valid = keySchema.zip(otherHashMapBlock.keySchema).forall(i => i._1.dataType == i._2.dataType) &&
          blockSchema.zip(otherHashMapBlock.blockSchema).forall(i => i._1.dataType == i._2.dataType)
        if(!valid){
          throw new IllegalArgumentException("the keySchemas or blockSchemas of the two blocks are not consistent")
        }
        val otherHashMap = otherHashMapBlock.hashMap
        for(key <- otherHashMap.keys){
          if(tempHashMap.contains(key)){
            tempHashMap(key) = tempHashMap(key) ++ otherHashMap(key)
          } else
          {
            tempHashMap.put(key, otherHashMap(key))
          }
        }
        val hashMap = HashMap[InternalRow, Array[InternalRow]](tempHashMap.toSeq:_*)
        new HashMapInternalBlock(hashMap, keySchema, blockSchema)
      case _ =>
        val valid = schema().zip(other.schema()).forall(i => i._1.dataType == i._2.dataType)
        if(!valid){
          throw new IllegalArgumentException("the schema() of the two blocks are not consistent")
        }
        val tempItemSeq = tempHashMap.toSeq.map{case(key, array) => (key, array.toBuffer)}
        val bufferHashMap = mutable.HashMap[InternalRow, mutable.Buffer[InternalRow]](tempItemSeq:_*)
        val projectFunc = GenerateUnsafeProjection.generate(keySchema, blockSchema)
        for (row <- other.iterator) {
          val key = projectFunc(row)
          if (bufferHashMap.contains(key))
            bufferHashMap(key.copy()).append(row.copy())
          else
            bufferHashMap.put(key.copy(), ArrayBuffer[InternalRow](row.copy()))
        }
        val itemSeq = bufferHashMap.toSeq.map{case(key, buffer) => (key, buffer.toArray)}
        val hashMap = HashMap[InternalRow, Array[InternalRow]](itemSeq:_*)
        new HashMapInternalBlock(hashMap, keySchema, blockSchema)
    }
  }

  /** Partition an [[InternalBlock]] into multiple [[InternalBlock]]s based on a partitioner.
    *
    * @param partitioner the partitioner used to partition the [[InternalBlock]]
    * @return an array of partitioned [[InternalBlock]]s
    */
  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  /** Show the first `num` rows */
  override def show(num: Int): Unit = {
    val showIterator = this.iterator.take(num)
    while (showIterator.hasNext) {
      val row = showIterator.next()
      if (row.isInstanceOf[GenericInternalRow]) println(row)
      else row.asInstanceOf[UnsafeInternalRow].show(StructType.fromAttributes(blockSchema))
    }
  }

  private lazy val rowArray = hashMap.values.flatten.toArray
  override def toArray(): Array[InternalRow] = rowArray

  override def contains(key: InternalRow): Boolean = hashMap.contains(key)

  override def get(key: InternalRow): Array[InternalRow] = hashMap.getOrElse(key, Array.empty[InternalRow])

  override def getDictionaryOrder: Option[Seq[String]] =
    throw new Exception("No dictionaryOrder in HashMapInternalBlock")
}

object HashMapInternalBlock extends LogAble {
  def apply(table: Array[InternalRow], schema: Array[Attribute], keySchema: Array[Attribute]): HashMapInternalBlock = {
    logInfo(s"keySchema: ${keySchema.mkString("Array(", ", ", ")")}")
    if(table.isEmpty){
      new HashMapInternalBlock(HashMap[InternalRow, Array[InternalRow]](), schema, keySchema)
    }
    val mutableHashMap = new mutable.HashMap[InternalRow, mutable.Buffer[InternalRow]]()
    val projectFunc = GenerateUnsafeProjection.generate(keySchema, schema)
    for (row <- table) {
      val key = projectFunc(row)
      if (mutableHashMap.contains(key))
        mutableHashMap(key).append(row)
      else
        mutableHashMap.put(key.copy(), ArrayBuffer[InternalRow](row))
    }
    val itemSeq = mutableHashMap.toSeq.map{case(key, arrayBuffer) => (key, arrayBuffer.toArray)}
    val immutableHashMap = HashMap[InternalRow, Array[InternalRow]](itemSeq:_*)
    logInfo(s"immutableHashMap: $immutableHashMap")
    new HashMapInternalBlock(immutableHashMap, schema, keySchema)
  }

  def builder(schema: Array[Attribute], keySchema: Array[Attribute]): HashMapInternalBlockBuilder =
    new HashMapInternalBlockBuilder(schema, keySchema)
}

class HashMapInternalBlockBuilder(schema: Array[Attribute], keySchema: Array[Attribute])
    extends InternalBlockBuilder {
  private val rows: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()

  /** Add a new row to builder. */
  override def add(row: InternalRow): Unit = rows.append(row)

  /** Build the InternalBlock. */
  override def build(): HashMapInternalBlock =
    HashMapInternalBlock(rows.toArray, schema, keySchema)
}
