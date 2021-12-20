package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.types._
import org.apache.spark.secco.util.BSearch

import java.util.Comparator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}

class StringNodeComparator extends Comparator[StringNode] with Serializable {

  override def compare(
      o1: StringNode,
      o2: StringNode
  ): Int = {

    if (o1.strValue < o2.strValue) return -1
    else if (o1.strValue > o2.strValue) return 1
    else return 0
  }
}

class TrieInternalBlock(
    schema: StructType,
    private var valuesAddress: Long,
    private var variableLengthZoneAddress: Long
) extends InternalBlock
    with TrieLike
    with RowLike {

  self =>

  private val level: Int = schema.length

  private var rootBegin: Int = _
  private var rootEnd: Int = _

  private val isString: Array[Boolean] =
    schema.map(item => item.dataType == StringType).toArray

  private var rowNum: Int = _

  private var fixedLengthZoneSize: Int = _

  private var currentUsedVariableLengthZoneSize: Int = _
  private var variableLengthZoneSize: Int = _

  private def alterFixedLengthZone(address: Long, size: Int): Unit = {
    val usedFixedLengthSize = numElems * elemSize
    assert(usedFixedLengthSize <= size, "New size is not large enough.")
    Utils.copyMemory(null, valuesAddress, null, address, usedFixedLengthSize)
    Utils._UNSAFE.freeMemory(valuesAddress)
    this.valuesAddress = address
    fixedLengthZoneSize = size
  }

  private def alterVariableLengthZone(address: Long, size: Int): Unit = {
    assert(
      currentUsedVariableLengthZoneSize <= size,
      "New size is not large enough."
    )
    Utils.copyMemory(
      null,
      variableLengthZoneAddress,
      null,
      address,
      currentUsedVariableLengthZoneSize
    )
    Utils._UNSAFE.freeMemory(variableLengthZoneAddress)
    this.variableLengthZoneAddress = address
    variableLengthZoneSize = size
  }

  override def getDictionaryOrder: Option[Seq[String]] = Some(schema.names)

  override def containPrefix(key: InternalRow): Boolean = {

    assert(key.numFields < this.level, "The key is too long.")

    var start = rootBegin
    var end = rootEnd
    var j = 0
    val bindingLevel = key.numFields
    var pos = 0

    while (j < bindingLevel) {

      if (!isString(j)) {
        val keyVal = key.get(j, schema(j).dataType)
        val keyLong = Utils.anyToLongForComparison(keyVal)
        pos = BSearch.searchUnsafe(valuesAddress, keyLong, start, end, elemSize)
      } else {
        val keyString = key.getString(j)
        pos = BSearch.searchUnsafeString(
          valuesAddress,
          variableLengthZoneAddress,
          keyString,
          start,
          end,
          elemSize
        )
      }

      if (pos == -1) {
        return false
      }

      start = getFirstChildIndex(pos)
      end = getFirstChildIndex(pos + 1)
      j += 1
    }

    return true
  }

  private val elemSize = 16
  private var numElems: Int = _
  private val getters = new Array[Int => Any](this.level)
  private def getString(idx: Int) =
    Utils.getStringAtColumnAddress(
      valuesAddress,
      variableLengthZoneAddress,
      idx,
      elemSize
    )
  private def getBoolean(idx: Int) =
    Utils._UNSAFE.getBoolean(null, valuesAddress + idx * elemSize)
  private def getInt(idx: Int) =
    Utils._UNSAFE.getInt(valuesAddress + idx * elemSize + 4)
  private def getFloat(idx: Int) =
    Utils._UNSAFE.getFloat(valuesAddress + idx * elemSize + 4)
  private def getLong(idx: Int) =
    Utils._UNSAFE.getLong(valuesAddress + idx * elemSize)
  private def getDouble(idx: Int) =
    Utils._UNSAFE.getDouble(valuesAddress + idx * elemSize)
  private def getParentIndex(idx: Int) =
    Utils._UNSAFE.getLong(valuesAddress + idx * elemSize + 8).toInt
  private def getFirstChildIndex(idx: Int) =
    (Utils._UNSAFE.getLong(valuesAddress + idx * elemSize + 8) >> 32).toInt
  //  getters(0) = getString

  private val setters = new Array[(Int, Any) => Unit](this.level)
  private def setBoolean(idx: Int, value: Any): Unit =
    Utils._UNSAFE.putBooleanVolatile(
      null,
      valuesAddress + idx * elemSize,
      value.asInstanceOf[Boolean]
    )
  private def setInt(idx: Int, value: Any): Unit =
    Utils._UNSAFE
      .putInt(valuesAddress + idx * elemSize, value.asInstanceOf[Int])
  private def setFloat(idx: Int, value: Any): Unit =
    Utils._UNSAFE
      .putFloat(valuesAddress + idx * elemSize + 4, value.asInstanceOf[Float])
  private def setLong(idx: Int, value: Any): Unit =
    Utils._UNSAFE
      .putLong(valuesAddress + idx * elemSize, value.asInstanceOf[Long])
  private def setDouble(idx: Int, value: Any): Unit =
    Utils._UNSAFE
      .putDouble(valuesAddress + idx * elemSize, value.asInstanceOf[Double])
  private def setString(idx: Int, value: Any): Unit = {
    val strValue = value.asInstanceOf[String]
    val stringSize = strValue.length
    //lgh: In case there is not enough space in variable-length zone.
    if (
      currentUsedVariableLengthZoneSize + stringSize > variableLengthZoneSize
    ) {
      val (newAddress, newSize) = Utils.grow(
        variableLengthZoneAddress,
        currentUsedVariableLengthZoneSize,
        stringSize
      )
      variableLengthZoneAddress = newAddress
      variableLengthZoneSize = newSize
    }
    val stringOffset: Long = currentUsedVariableLengthZoneSize.toLong
    val offsetAndSize = stringOffset << 32 | stringSize.toLong
    Utils._UNSAFE.putLong(null, valuesAddress + idx * elemSize, offsetAndSize)
    val dataArray = strValue.getBytes()
    Utils.copyMemory(
      dataArray,
      Utils._UNSAFE.arrayBaseOffset(dataArray.getClass),
      null,
      variableLengthZoneAddress + stringOffset,
      stringSize
    )
    currentUsedVariableLengthZoneSize += stringSize
  }
  private def setParentIndex(idx: Int, parentIndex: Int): Unit =
    Utils._UNSAFE.putInt(valuesAddress + idx * elemSize + 8, parentIndex)
  private def setFirstChildIndex(idx: Int, firstChildIndex: Int): Unit =
    Utils._UNSAFE.putInt(valuesAddress + idx * elemSize + 12, firstChildIndex)
  private def setFirstChildIndexAndParentIndex(
      idx: Int,
      indexWord: Long
  ): Unit =
    Utils._UNSAFE.putLong(valuesAddress + idx * elemSize + 8, indexWord)

  def getString(key: InternalRow): Array[String] = get(key).map(_.asInstanceOf[String])
  def getBoolean(key: InternalRow): Array[Boolean] = get(key).map(_.asInstanceOf[Boolean])
  def getInt(key: InternalRow): Array[Int] = get(key).map(_.asInstanceOf[Int])
  def getLong(key: InternalRow): Array[Long] = get(key).map(_.asInstanceOf[Long])
  def getDouble(key: InternalRow): Array[Double] = get(key).map(_.asInstanceOf[Double])
  def getFloat(key: InternalRow): Array[Float] = get(key).map(_.asInstanceOf[Float])

  override def get(key: InternalRow): Array[Any] = {

    var start = rootBegin
    var end = rootEnd
    var j = 0
    val bindingLevel = key.numFields
    var pos = 0

    while (j < bindingLevel) {

      if (!isString(j)) {
        val keyVal = key.get(j, schema(j).dataType)
        val keyLong = Utils.anyToLongForComparison(keyVal)
        pos = BSearch.searchUnsafe(valuesAddress, keyLong, start, end, elemSize)
      } else {
        val keyString = key.getString(j)
        pos = BSearch.searchUnsafeString(
          valuesAddress,
          variableLengthZoneAddress,
          keyString,
          start,
          end,
          elemSize
        )
      }

      if (pos == -1) return Array[Any]()

      start = getFirstChildIndex(pos)
      end = getFirstChildIndex(pos + 1)
      j += 1
    }

    assert(j == bindingLevel, "j != bindingLevel")

    val arrayBuffer = new ArrayBuffer[Any]
    for (idx <- start until end)
      arrayBuffer.append(getters(j)(idx).asInstanceOf[Any])
    arrayBuffer.toArray
  }

  override def toArray(): Array[InternalRow] = {
    var tables =
      get(InternalRow.empty).map(f => Array(f))

    var i = 1
    while (i < level) {
      tables = tables.flatMap { f =>
        val nextLevelValues = get(InternalRow(f: _*))
        nextLevelValues.map(value => f :+ value)
      }

      i += 1
    }

    tables.map(f => InternalRow(f: _*))
  }

  //  override def toString: String = {
  //    s"""
  //       |== Array Trie ==
  //       |neighbors:${neighbors.toSeq}
  //       |values:${values.toSeq}
  //       |neighborStart:${neighborBegins.toSeq}
  //       |neighborEnd:${neighborEnds.toSeq}
  //     """.stripMargin
  //  }

  private var iterIndex = 0
  private var endIdxArray = new Array[Int](this.level)
  private var idxArray = new Array[Int](this.level)

  {
    idxArray(0) = rootBegin
    endIdxArray(0) = rootEnd
    for (j <- 0 until level - 1) {
      idxArray(j + 1) = getFirstChildIndex(idxArray(j))
      endIdxArray(j + 1) = getFirstChildIndex(idxArray(j) + 1)
    }
  }

  private def setRowWithIdxArray(idxArray: Array[Int], row: InternalRow): Unit =
    for (j <- 0 until level) row(j) = getters(j)(idxArray(j))

  private val row: InternalRow = InternalRow(new Array[Any](level): _*)

  /** The iterator for accessing InternalBlock.
    * Note: The [[InternalRow]] returned by this class will be reused.
    */
  override def iterator: Iterator[InternalRow] =
    new Iterator[InternalRow] {
      override def hasNext: Boolean = idxArray(0) < endIdxArray(0)

      override def next(): InternalRow = {
        setRowWithIdxArray(idxArray, row)

        var j = level
        do {
          j -= 1
          idxArray(j) += 1
        } while (j > 0 && idxArray(j) >= endIdxArray(j))

        if (hasNext) {
          while (j < level - 1) {
            idxArray(j + 1) = getFirstChildIndex(idxArray(j))
            endIdxArray(j + 1) = getFirstChildIndex(idxArray(j) + 1)
            j += 1
          }
        }

        return row
      }
    }

  /** Return the numbers of rows of [[InternalBlock]] */
  override def size(): Long = rowNum

  /** Return true if the [[InternalBlock]] is empty */
  override def isEmpty(): Boolean = rowNum == 0

  /** Return true if the [[InternalBlock]] is non-empty */
  override def nonEmpty(): Boolean = rowNum != 0

  /** Return the schema of [[InternalBlock]] */
  override def schema(): StructType = schema

  /** Sort the rows by an dictionary order.
    *
    * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
    * @return a new sorted InternalBlock.
    */
  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock =
    TrieInternalBlock(self.toArray(), schema, Some(DictionaryOrder))

  /** Merge two (sorted) [[InternalBlock]]
    *
    * @param other             the other [[InternalBlock]] to be merged
    * @param maintainSortOrder whether the sorting order in InternalBlock should be maintained in merged [[InternalBlock]]
    * @return an merged (sorted) [[InternalBlock]]
    */
  override def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean
  ): InternalBlock = {
    def schemasAreConsistent(): Boolean = {
      schema.length == other.schema().length && {
        for (j <- schema.indices)
          if (schema(j).dataType != other.schema()(j).dataType) return false
        true
      }
    }
    assert(
      schemasAreConsistent(),
      "Schemas of the two blocks are not consistent"
    )
    TrieInternalBlock(self.toArray() ++ other.toArray(), schema)
  }

  /** Partition an [[InternalBlock]] into multiple [[InternalBlock]]s based on a partitioner.
    *
    * @param partitioner the partitioner used to partition the [[InternalBlock]]
    * @return an array of partitioned [[InternalBlock]]s
    */
  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  /** Show the first `num` rows */
  override def show(num: Int): Unit = {
    print("row_index")
    schema.foreach(item => print(s"\t${item.name}"))
    print("\n")
    for (i <- 0 until num) {
      val row = getRow(i)
      print(i)
      for (j <- 0 until schema.length)
        print(s"\t${row.get(j, schema(j).dataType)}")
      print("\n")
    }
  }

  override def getRow(i: Int): InternalRow = {
    //    val array = new Array[Any](level)
    val row = InternalRow(new Array[Any](level): _*)

    var idx = numElems - rowNum + i
    for (j <- 0 until level) {
      val curColumnIdx = level - 1 - j
      row(curColumnIdx) = getters(curColumnIdx)(idx)
      idx = getParentIndex(idx)
    }

    return row
  }

  override def getRows(key: InternalRow): Array[InternalRow] = ???
}

//lgh: Store values in an Array in the order of level traversal.
object TrieInternalBlock {

  private def apply(
      table: Array[InternalRow],
      rowsSchema: StructType,
      dictionaryOrder: Option[Seq[String]]
  ): TrieInternalBlock = {

    def dictionaryOrderIsValid(): Boolean = {
      dictionaryOrder.get.length == rowsSchema.length &&
      dictionaryOrder.get.distinct.length == dictionaryOrder.get.length && {
        for (j <- rowsSchema.indices)
          if (!rowsSchema.names.contains(dictionaryOrder.get(j))) return false
        true
      }
    }
    if (dictionaryOrder.isDefined)
      assert(
        dictionaryOrderIsValid(),
        "The dictionaryOrder fails to cover all columns"
      )

    def buildMapArray(): Array[Int] = {
      val indexMapArray = new Array[Int](rowsSchema.length)
      if (dictionaryOrder.isEmpty)
        for (j <- indexMapArray) indexMapArray(j) = j
      else {
        val columnNamesOriginal: Seq[String] = rowsSchema.map(_.name)
        val columnNamesInOrder: Seq[String] =
          dictionaryOrder.getOrElse(columnNamesOriginal)
        for (j <- indexMapArray)
          indexMapArray(j) = columnNamesOriginal.indexOf(columnNamesInOrder(j))
      }
      indexMapArray
    }
    val indexNewToOldMapArray = buildMapArray()

    //lgh: Initialize a TrieLikeInternalBlock
    val arity = rowsSchema.length
    val blockFields = new Array[StructField](arity)
    for (j <- blockFields.indices)
      blockFields(j) = rowsSchema(indexNewToOldMapArray(j))
    val blockSchema = StructType(blockFields)
    val initialSize = table.length * 16
    val initialFixedLengthZoneAddress =
      Utils._UNSAFE.allocateMemory(initialSize.toLong)
    val initialVariableLengthZoneAddress =
      Utils._UNSAFE.allocateMemory(initialSize.toLong)
    val block = new TrieInternalBlock(
      blockSchema,
      initialFixedLengthZoneAddress,
      initialVariableLengthZoneAddress
    )
    block.fixedLengthZoneSize = initialSize
    block.variableLengthZoneSize = initialSize
    def initGettersAndSetters(): Unit = {
      for (j <- blockSchema.indices) {
        blockSchema(j).dataType match {
          case IntegerType =>
            block.getters(j) = block.getInt
            block.setters(j) = block.setInt
          case LongType =>
            block.getters(j) = block.getLong
            block.setters(j) = block.setLong
          case FloatType =>
            block.getters(j) = block.getFloat
            block.setters(j) = block.setFloat
          case DoubleType =>
            block.getters(j) = block.getDouble
            block.setters(j) = block.setDouble
          case BooleanType =>
            block.getters(j) = block.getBoolean
            block.setters(j) = block.setBoolean
          case StringType =>
            block.getters(j) = block.getString
            block.setters(j) = block.setString
          case _ =>
            throw new NotImplementedError(
              s"${rowsSchema(j).dataType} is currently not supported"
            )
        }
      }
    }
    initGettersAndSetters()

    val stringNodeBuffer = new ArrayBuffer[StringNode]

    def growFixedLengthZoneIfNeeded(): Unit = {
      if ((block.numElems + 1) * block.elemSize > block.fixedLengthZoneSize) {
        val (newAddress, newSize) = Utils.grow(
          block.valuesAddress,
          block.numElems * block.elemSize,
          block.elemSize
        )
        block.alterVariableLengthZone(newAddress, newSize)
      }
    }

    //lgh: sort all strings, compress fixed-length zone size and variable-length zone size
    def finalProcess(): Unit = {
      //sort all strings
      val stringNodeArray = stringNodeBuffer.toArray
      val stringNodeComparator = new StringNodeComparator
      java.util.Arrays.sort(stringNodeArray, stringNodeComparator)
      for (item <- stringNodeArray) {
        block.setString(item.idx, item.strValue)
      }
      val fixedLengthZoneSize = block.numElems * block.elemSize
      val fixedLengthZoneAddress =
        Utils._UNSAFE.allocateMemory(fixedLengthZoneSize)
      block.alterFixedLengthZone(fixedLengthZoneAddress, fixedLengthZoneSize)
      val variableLengthZoneSize = Utils.roundNumberOfBytesToNearestWord(
        block.currentUsedVariableLengthZoneSize
      )
      val variableLengthZoneAddress =
        Utils._UNSAFE.allocateMemory(variableLengthZoneSize)
      block.alterVariableLengthZone(
        variableLengthZoneAddress,
        variableLengthZoneSize
      )
    }

    if (arity < 1 || table.length < 1) return block

    //sort the relation in lexical order
    val comparator =
      new Utils.InternalRowComparator(rowsSchema, indexNewToOldMapArray)
    java.util.Arrays.sort(table, comparator)

    var idCounter = 0
    var parentIdCounter = 0

    var value =
      table.head.get(indexNewToOldMapArray(0), rowsSchema.head.dataType)
    idCounter += 1
    val curIsString = block.isString.head
    val curDataType = block.schema().head.dataType
    for (i <- 1 until table.length) {
      val curValue = table(i).get(indexNewToOldMapArray(0), curDataType)
      if (curValue != value) {
        if (!curIsString) {
          growFixedLengthZoneIfNeeded()
          block.setters(0)(idCounter, curValue)
        } else
          stringNodeBuffer.append(
            StringNode(idCounter, curValue.asInstanceOf[String])
          )
        block.numElems += 1
        idCounter += 1
        value = curValue
      }
    }

    if (arity < 2) {
      block.setFirstChildIndex(0, idCounter)
      finalProcess()
      return block
    }

    for (j <- 1 until arity) {
      val curIsString = block.isString(j)
      val curParentDataType = block.schema()(j - 1).dataType
      val curDataType = block.schema()(j).dataType
      var valueParent =
        table.head.get(indexNewToOldMapArray(j - 1), curParentDataType)
      parentIdCounter += 1
      var valueChild = table.head.get(indexNewToOldMapArray(j), curDataType)
      idCounter += 1

      def setCurValue(curValue: AnyRef): Unit = {
        if (!curIsString) {
          growFixedLengthZoneIfNeeded()
          block.setters(j)(idCounter, curValue)
        } else
          stringNodeBuffer.append(
            StringNode(idCounter, curValue.asInstanceOf[String])
          )
        block.setParentIndex(idCounter, parentIdCounter)
        block.numElems += 1
        idCounter += 1
        valueChild = curValue
      }

      for (i <- 1 until table.length) {
        val curParentValue =
          table(i).get(indexNewToOldMapArray(j - 1), curParentDataType)
        val curValue = table(i).get(indexNewToOldMapArray(j), curDataType)
        if (curParentValue != valueParent) {
          setCurValue(curValue)
          block.setFirstChildIndex(parentIdCounter, idCounter)
          valueParent = curParentValue
          parentIdCounter += 1
        } else if (curValue != valueChild)
          setCurValue(curValue)
      }
    }

    finalProcess()
    return block
  }

  def apply(table: Array[InternalRow], schema: StructType): TrieInternalBlock =
    apply(table, schema, None)

  def builder(schema: StructType): TrieInternalBlockBuilder =
    new TrieInternalBlockBuilder(schema)
}

class TrieInternalBlockBuilder(schema: StructType)
    extends InternalBlockBuilder {

  private val internalRowBuffer = ArrayBuffer[InternalRow]()

  /** Add a new row to builder. */
  override def add(row: InternalRow): Unit = internalRowBuffer.append(row)

  /** Build the InternalBlock. */
  override def build(): InternalBlock =
    TrieInternalBlock(internalRowBuffer.toArray, schema)
}

case class StringNode(idx: Int, strValue: String)




// lgh code fragments

// 1.
//abstract class UnsafeArray[T]{
//  def apply(i: Int): T
//  def update(i: Int, t: T): Unit
//}
//
//class UnsafeArrayLong(length: Int) extends UnsafeArray[Long]{
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length * 8)
//
//  def apply(i: Int): Long = Utils._UNSAFE.getLong(baseAddress + i * 8)
//
//  def update(i: Int, t: Long): Unit = Utils._UNSAFE.putLong(baseAddress + i * 8, t)
//
//  override def finalize(): Unit = {
//    Utils._UNSAFE.freeMemory(baseAddress)
//    super.finalize()
//  }
//}
//
//object UnsafeArrayLong {
//  def apply(xs: Long*): UnsafeArrayLong = {
//    val array = new UnsafeArrayLong(xs.length)
//    var i = 0
//    for (x <- xs.iterator) { array(i) = x; i += 1 }
//    array
//  }
//}
//
//class UnsafeArrayInt(length: Int) extends UnsafeArray[Int]{
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length * 4)
//
//  def apply(i: Int): Int = Utils._UNSAFE.getInt(baseAddress + i * 4)
//
//  def update(i: Int, t: Int): Unit = Utils._UNSAFE.putInt(baseAddress + i * 4, t)
//
//  override def finalize(): Unit = {
//    Utils._UNSAFE.freeMemory(baseAddress)
//    super.finalize()
//  }
//}
//
//object UnsafeArrayInt {
//  def apply(xs: Int*): UnsafeArrayInt = {
//    val array = new UnsafeArrayInt(xs.length)
//    var i = 0
//    for (x <- xs.iterator) { array(i) = x; i += 1 }
//    array
//  }
//}
//
//class UnsafeArrayDouble(length: Int) extends UnsafeArray[Double]{
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length * 8)
//
//  def apply(i: Int): Double = Utils._UNSAFE.getDouble(baseAddress + i * 8)
//
//  def update(i: Int, t: Double): Unit = Utils._UNSAFE.putDouble(baseAddress + i * 8, t)
//
//  override def finalize(): Unit = {
//    Utils._UNSAFE.freeMemory(baseAddress)
//    super.finalize()
//  }
//}
//
//object UnsafeArrayDouble {
//  def apply(xs: Int*): UnsafeArrayDouble = {
//    val array = new UnsafeArrayDouble(xs.length)
//    var i = 0
//    for (x <- xs.iterator) { array(i) = x; i += 1 }
//    array
//  }
//}
//
//class UnsafeArrayFloat(length: Int) extends UnsafeArray[Float]{
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length * 4)
//
//  def apply(i: Int): Float = Utils._UNSAFE.getFloat(baseAddress + i * 4)
//
//  def update(i: Int, t: Float): Unit = Utils._UNSAFE.putFloat(baseAddress + i * 4, t)
//
//  override def finalize(): Unit = {
//    Utils._UNSAFE.freeMemory(baseAddress)
//    super.finalize()
//  }
//}
//
//object UnsafeArrayFloat {
//  def apply(xs: Int*): UnsafeArrayFloat = {
//    val array = new UnsafeArrayFloat(xs.length)
//    var i = 0
//    for (x <- xs.iterator) { array(i) = x; i += 1 }
//    array
//  }
//}
//
//class UnsafeArrayBoolean(length: Int) extends UnsafeArray[Boolean]{
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length)
//
//  def apply(i: Int): Boolean = Utils._UNSAFE.getBoolean(null, baseAddress + i)
//
//  def update(i: Int, t: Boolean): Unit = Utils._UNSAFE.putBoolean(null, baseAddress + i, t)
//
//  override def finalize(): Unit = {
//    Utils._UNSAFE.freeMemory(baseAddress)
//    super.finalize()
//  }
//}
//
//object UnsafeArrayBoolean {
//  def apply(xs: Boolean*): UnsafeArrayBoolean = {
//    val array = new UnsafeArrayBoolean(xs.length)
//    var i = 0
//    for (x <- xs.iterator) { array(i) = x; i += 1 }
//    array
//  }
//}

//
//class UnsafeArrayLong(length: Int) extends ArrayLike[Long] {
//
//  private val baseAddress = Utils._UNSAFE.allocateMemory(length * 8)
//
//  override def apply(i: Int): Long = Utils._UNSAFE.getLong(baseAddress + i * 8)
//
//  override def update(i: Int, t: Long): Unit = Utils._UNSAFE.putLong(baseAddress + i * 8, t)
//
//  override def valuesIterator: Iterator[Long] = new Iterator[Long] {
//    override def hasNext: Boolean = ???
//
//    override def next(): Long = ???
//  }
//
//  override def keysIterator: Iterator[Int] = ???
//
//  override def activeSize: Int = ???
//
//  override def size: Int = length
//}




// 2.
//override def get[T: ClassTag](key: InternalRow): Array[T] = {
//
//  var start = rootBegin
//  var end = rootEnd
//  var j = 0
//  val bindingLevel = key.numFields
//  var pos = 0
//
//  while (j < bindingLevel) {
//
//  if (!isString(j)) {
//  val keyVal = key.get(j, schema(j).dataType)
//  val keyLong = Utils.anyToLongForComparison(keyVal)
//  pos = BSearch.searchUnsafe(valuesAddress, keyLong, start, end, elemSize)
//  } else {
//  val keyString = key.getString(j)
//  pos = BSearch.searchUnsafeString(
//  valuesAddress,
//  variableLengthZoneAddress,
//  keyString,
//  start,
//  end,
//  elemSize
//  )
//  }
//
//  if (pos == -1) return Array[T]()
//
//  start = getFirstChildIndex(pos)
//  end = getFirstChildIndex(pos + 1)
//  j += 1
//  }
//
//  assert(j == bindingLevel, "j != bindingLevel")
//
//  val arrayBuffer = new ArrayBuffer[T]
//  for (idx <- start until end)
//  arrayBuffer.append(getters(j)(idx).asInstanceOf[T])
//  arrayBuffer.toArray
//  }




// 3.
//override def toArray(): Array[InternalRow] = {
//  var tables =
//  get[Any](InternalRow.empty).map(f => Array(f))
//
//  var i = 1
//  while (i < level) {
//  tables = tables.flatMap { f =>
//  val nextLevelValues = get[Any](InternalRow(f: _*))
//  nextLevelValues.map(value => f :+ value)
//  }
//
//  i += 1
//  }
//
//  tables.map(f => InternalRow(f: _*))
//  }