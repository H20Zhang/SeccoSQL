package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.Utils
import org.apache.spark.secco.execution.storage.row.{
  GenericInternalRow,
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.types._
import sun.misc.Unsafe

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer

/** The InternalBlock backed by UnsafeInternalRows */
class UnsafeInternalRowBlock
    extends InternalBlock
    with RowLike
    with UnsafeHelper {
  private var dictionaryOrder: Option[Seq[String]] = None
  private var baseOffset: Long = _
  private var variableBaseOffset: Array[Long] =
    _ // store the its baseAddress if it's a variableLength area, else null
  private var blockSchema: StructType = _
  private var variableLengthRecord: Array[Long] = _
  // private var getters = Array[(Long, Int, Long) => Any]
  // private var gettersArray[(Long, Int, Long) => Any]rray[FunctionManager[T]] = _
  private var bitMapSize: Int = _
  private var rowSize: Long = _
  private var blockSize: Int = _

  def this(
      baseoffset: Long,
      variableBaseOffset: Array[Long],
      variableLengthRecord: Array[Long],
      blockSchema: StructType,
      size: Int
  ) {
    this()
    this.baseOffset = baseoffset
    this.variableBaseOffset = variableBaseOffset.clone()
    this.blockSchema = blockSchema
    this.variableLengthRecord = variableLengthRecord
    this.bitMapSize = calculateBitMapWidthInWords(blockSchema.fields.length) * 8
    // this.getters = determineFieldGetter(blockSchema)
    this.rowSize = blockSchema.fields.length * fieldByteSize + bitMapSize
    this.blockSize = size
  }

  private def getBoolean(address: Long, ordinal: Int): Boolean =
    _UNSAFE.getBoolean(null, address + ordinal * fieldByteSize + bitMapSize)

  private def getInt(address: Long, ordinal: Int): Int =
    _UNSAFE.getInt(null, address + ordinal * fieldByteSize + bitMapSize)

  private def getLong(address: Long, ordinal: Int): Long =
    _UNSAFE.getLong(null, address + ordinal * fieldByteSize + bitMapSize)

  private def getFloat(address: Long, ordinal: Int): Float =
    _UNSAFE.getFloat(null, address + ordinal * fieldByteSize + bitMapSize)

  private def getDouble(address: Long, ordinal: Int): Double =
    _UNSAFE.getDouble(null, address + ordinal * fieldByteSize + bitMapSize)

  private def getString(address: Long, ordinal: Int): String = {
    val offsetAndSize: Long = getLong(address, ordinal)
    val offset: Long = offsetAndSize >>> 32
    val size: Int = offsetAndSize.toInt
    val buf: Array[Byte] = new Array[Byte](size)
    copyMemory(
      null,
      variableBaseOffset(ordinal) + offset,
      buf,
      BYTE_ARRAY_OFFSET,
      size
    )
    new String(buf)
  }

  private def getString(
      address: Long,
      theVariableBaseOffset: Array[Long],
      ordinal: Int
  ): String = {
    val offsetAndSize: Long = getLong(address, ordinal)
    val offset: Long = offsetAndSize >>> 32
    val size: Int = offsetAndSize.toInt
    val buf: Array[Byte] = new Array[Byte](size)
    copyMemory(
      null,
      theVariableBaseOffset(ordinal) + offset,
      buf,
      BYTE_ARRAY_OFFSET,
      size
    )
    new String(buf)
  }

  //  private def getAny(address: Long, ordinal: Int): Any = _UNSAFE.getLong(null, address + ordinal * fieldByteSize + bitMapSize)

  override def getRow(i: Int): InternalRow = {
    getRowFromBaseOffset(baseOffset, variableBaseOffset, i)
    //    val unsafeInternalRow = new UnsafeInternalRow(blockSchema.fields.length)
    //    copyMemory(null, baseOffset + i * rowSize, null, unsafeInternalRow.baseOffset, rowSize) // copy the data of fixed-length area
    //    val variableFields = schema().fields.filter(_.dataType == StringType)
    //    for (index <- variableFields.indices) {
    //      val ordinal = blockSchema.fields.indexOf(variableFields(index)) // the field index in row
    //
    //      if (!unsafeInternalRow.isNullAt(ordinal)) {
    //        val vStr = getString(baseOffset + i * rowSize, ordinal)
    //        unsafeInternalRow.setNullAt(ordinal)
    //        unsafeInternalRow.setString(ordinal, vStr) // set the variable-length data
    //      }
    //    }
    //    unsafeInternalRow
  }

  private def getRowFromBaseOffset(
      theBaseOffset: Long,
      theVariableBaseOffset: Array[Long],
      i: Int
  ): UnsafeInternalRow = {
    val unsafeInternalRow =
      new UnsafeInternalRow(blockSchema.fields.length, true)
    copyMemory(
      null,
      theBaseOffset + i * rowSize,
      null,
      unsafeInternalRow.baseOffset,
      rowSize
    ) // copy the data of fixed-length area
    val variableFields = schema().fields.filter(_.dataType == StringType)
    for (index <- variableFields.indices) {
      val ordinal =
        blockSchema.indexOf(variableFields(index)) // the field index in
      if (!unsafeInternalRow.isNullAt(ordinal)) {
        val vStr =
          getString(theBaseOffset + i * rowSize, theVariableBaseOffset, ordinal)
        unsafeInternalRow.setNullAt(ordinal)
        unsafeInternalRow.setString(
          ordinal,
          vStr
        ) // set the variable-length data
      }
    }
    unsafeInternalRow
  }

  override def iterator: Iterator[InternalRow] =
    new AbstractIterator[InternalRow] {
      var index: Int = 0

      override def hasNext: Boolean = index < blockSize

      override def next(): InternalRow = {
        val unsafeInternalRow = getRow(index)
        index += 1
        unsafeInternalRow
      }
    }

  override def size(): Long = blockSize

  override def isEmpty(): Boolean = blockSize == 0

  override def nonEmpty(): Boolean = blockSize != 0

  override def schema(): StructType = blockSchema

  /** This method will allocate a new memory space and copy the data from the old Block first,
    * and then sort will be executed in the new memory space.
    *
    * @param DictionaryOrder the dictionary orders to be used to sort InternalRow.
    * @return a new sorted InternalBlock.
    */
  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock = {
    val priority =
      new Array[Int](
        DictionaryOrder.length
      ) // store the index of the field that needs to be sorted
    val fields = blockSchema.fields
    this.dictionaryOrder = Option(DictionaryOrder)
    // copy fixed-length data to new baseOffset
    val newBaseOffset = _UNSAFE.allocateMemory(blockSize * rowSize)
    copyMemory(null, baseOffset, null, newBaseOffset, blockSize * rowSize)
    // copy variable-length data to new variableBaseOffset
    val newVariableBaseOffset = new Array[Long](variableBaseOffset.length)
    val newVariableLengthRecord = new Array[Long](variableLengthRecord.length)
    for (index <- fields.indices) {
      val dataType = fields(index).dataType
      if (dataType == StringType) {
        newVariableBaseOffset(index) =
          _UNSAFE.allocateMemory(variableLengthRecord(index))
        newVariableLengthRecord(index) = variableLengthRecord(index)
        copyMemory(
          null,
          variableBaseOffset(index),
          null,
          newVariableBaseOffset(index),
          variableLengthRecord(index)
        )
      }
    }

    var gotIndex = 0
    for (i <- fields.indices) {
      val name = fields(i).name
      if (DictionaryOrder.contains(name)) {
        priority(gotIndex) = i
        gotIndex += 1
      }
    }
    quickSort(
      newBaseOffset,
      priority,
      0,
      blockSize
    ) // sort in the new memory space
    val newUnsafeInternalBlock = new UnsafeInternalRowBlock(
      newBaseOffset,
      newVariableBaseOffset,
      newVariableLengthRecord,
      blockSchema,
      blockSize
    )
    newUnsafeInternalBlock.setDictionaryOrder(Option(DictionaryOrder))
    newUnsafeInternalBlock
  }

  private def setDictionaryOrder(dictionaryOrder: Option[Seq[String]]): Unit = {
    this.dictionaryOrder = dictionaryOrder
  }

  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  override def show(num: Int): Unit = {
    for (i <- 0 until num) {
      val row = getRow(i)
      if (row.isInstanceOf[GenericInternalRow]) println(row)
      else row.asInstanceOf[UnsafeInternalRow].show(blockSchema)
    }
  }

  override def toArray(): Array[InternalRow] = {
    val blockArray = new Array[InternalRow](blockSize)
    for (i <- 0 until blockSize) {
      blockArray(i) = getRow(i)
    }
    blockArray
  }

  private def largerThan(
      theBaseOffset: Long,
      priority: Array[Int],
      i: Int,
      otherInternalRow: InternalRow,
      theVariableBaseOffset: Array[Long],
      index: Int = 0
  ): Boolean = {
    if (index == priority.length) return false // comparison ends
    val castType = schema().fields(priority(index)).dataType
    val ordinal = priority(index)
    castType match {
      case DoubleType =>
        val value_i = getDouble(theBaseOffset + i * rowSize, ordinal)
        val value_j = otherInternalRow.getDouble(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case FloatType =>
        val value_i = getFloat(theBaseOffset + i * rowSize, ordinal)
        val value_j = otherInternalRow.getFloat(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case IntegerType =>
        val value_i = getInt(theBaseOffset + i * rowSize, ordinal)
        val value_j = otherInternalRow.getInt(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case LongType =>
        val value_i = getLong(theBaseOffset + i * rowSize, ordinal)
        val value_j = otherInternalRow.getLong(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case StringType =>
        val value_i =
          getString(theBaseOffset + i * rowSize, theVariableBaseOffset, ordinal)
        val value_j = otherInternalRow.getString(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case BooleanType =>
        val value_i = getBoolean(theBaseOffset + i * rowSize, ordinal)
        val value_j = otherInternalRow.getBoolean(ordinal)
        if (value_i > value_j) {
          return true
        } else if (value_i == value_j) {
          return largerThan(
            theBaseOffset,
            priority,
            i,
            otherInternalRow,
            theVariableBaseOffset,
            index + 1
          )
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    false
  }

  /** This method merges two InternalBlock, it will allocate a new memory space first, and the variable-length
    * data will be copied, but the fixed-length data only will be copied when it's needed according to the contrary
    * two pointer merge-sort algorithm.
    *
    * @param other             the other [[InternalBlock]] to be merged
    * @param maintainSortOrder whether the sorting order in InternalBlock should be maintained in merged [[InternalBlock]]
    * @return an merged (sorted) [[InternalBlock]]
    */
  override def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean
  ): InternalBlock = {
    val variableFields = schema().fields.filter(_.dataType == StringType)
    val variableNum = variableFields.length
    val indexOfVField = new Array[Int](variableNum)
    for (index <- 0 until variableNum) {
      indexOfVField(index) = schema().fields.indexOf(variableFields(index))
    }
    val otherIterator = other.iterator
    if (maintainSortOrder) {
      // check if both InternalBlocks are sorted
      assert(dictionaryOrder.nonEmpty, "the source block is not sorted!")
      assert(
        other.getDictionaryOrder.nonEmpty,
        "the target block is not sorted!"
      )
      assert(
        dictionaryOrder.equals(other.getDictionaryOrder),
        "the two blocks are not sorted in the same way!"
      )

      // allocate new memory space for fixed-length data of the new block
      val newBaseOffset =
        _UNSAFE.allocateMemory((size() + other.size()) * rowSize)

      // allocate new memory space for variable-length data of the new block
      // copy variable-length data of source block to new variableBaseOffset
      val newVariableBaseOffset = new Array[Long](variableBaseOffset.length)
      val newVariableLengthRecord = new Array[Long](variableLengthRecord.length)
      for (index <- indexOfVField) {
        newVariableBaseOffset(index) =
          _UNSAFE.allocateMemory(variableLengthRecord(index))
        newVariableLengthRecord(index) = variableLengthRecord(index)
        copyMemory(
          null,
          variableBaseOffset(index),
          null,
          newVariableBaseOffset(index),
          variableLengthRecord(index)
        )
      }

      // copy the variable-length data from row to the variable-length area of block
      // we getString from the row and set it into the block variable-length area
      for (i <- 0 until other.size().toInt) {
        val otherInternalRow = otherIterator.next().copy()
        for (j <- 0 until variableNum) {
          val ordinal = indexOfVField(j)
          val targetStringBA =
            otherInternalRow.getString(ordinal).map(_.toByte).toArray
          val offset: Long = newVariableLengthRecord(ordinal)
          val stringSize = targetStringBA.length
          newVariableLengthRecord(ordinal) += stringSize
          newVariableBaseOffset(ordinal) = _UNSAFE.reallocateMemory(
            newVariableBaseOffset(ordinal),
            newVariableLengthRecord(ordinal)
          )
          setString(
            newVariableBaseOffset(ordinal) + offset,
            targetStringBA,
            stringSize
          )
          // data has been copied, then we store long value in the corresponding field to represent the offset and size
          val offsetAndSize = (offset << 32) + stringSize
          otherInternalRow.setLong(ordinal, offsetAndSize)
        }
        // copy the fixed-length data of the row to the fixed-length data area of block
        copyDataFromInternalRow(newBaseOffset, i, otherInternalRow)
      }
      val sortOrder = dictionaryOrder.get
      val priority = new Array[Int](sortOrder.size)
      for (index <- sortOrder.indices) {
        val fieldName = sortOrder(index)
        val field = schema().fields.filter(_.name == fieldName)(0)
        val indexInRow = schema().fields.indexOf(field)
        priority(index) = indexInRow
      }
      // begin the contrary two pointer merge-sort algorithm
      var p1 = other.size().toInt - 1
      // var tmpP1 = p1
      var p2 = size().toInt - 1
      //      var otherInternalRow_p1 = otherIterator.next()
      var tail = (size() + other.size() - 1).toInt
      while (p1 >= 0 || p2 >= 0) {
        // the row get from source block can be definitely converted to unsafeInternalRow
        if (p1 == (-1)) {
          // use fast copy for unsafeInternalRow
          // val unsafeInternalRow_p2 = this.getRow(p2).asInstanceOf[UnsafeInternalRow]
          copyDataFromUnsafeInternalRow(newBaseOffset, tail, baseOffset, p2)
          p2 -= 1
        } else if (p2 == (-1)) {
          // val otherUnsafeInternalRow_p1 = getRowFromBaseOffset(newBaseOffset, newVariableBaseOffset, p1)
          copyDataFromUnsafeInternalRow(newBaseOffset, tail, newBaseOffset, p1)
          p1 -= 1
        } else {
          val unsafeInternalRow_p2 =
            this.getRow(p2).asInstanceOf[UnsafeInternalRow]
          // val otherUnsafeInternalRow_p1 = getRowFromBaseOffset(newBaseOffset, newVariableBaseOffset, p1)
          if (
            largerThan(
              newBaseOffset,
              priority,
              p1,
              unsafeInternalRow_p2,
              newVariableBaseOffset
            )
          ) {
            copyDataFromUnsafeInternalRow(
              newBaseOffset,
              tail,
              newBaseOffset,
              p1
            )
            p1 -= 1
          } else {
            copyDataFromUnsafeInternalRow(newBaseOffset, tail, baseOffset, p2)
            p2 -= 1
          }
        }
        tail -= 1
      }
      val newBlockSize = blockSize + other.size().toInt
      new UnsafeInternalRowBlock(
        newBaseOffset,
        newVariableBaseOffset,
        newVariableLengthRecord,
        blockSchema,
        newBlockSize
      )
    } else {
      // copy fixed-length data of source block to new baseOffset
      val newBaseOffset =
        _UNSAFE.allocateMemory((size() + other.size()) * rowSize)
      copyMemory(null, baseOffset, null, newBaseOffset, blockSize * rowSize)
      // copy variable-length data of source block to new variableBaseOffset
      val newVariableBaseOffset = new Array[Long](variableBaseOffset.length)
      val newVariableLengthRecord = new Array[Long](variableLengthRecord.length)
      for (index <- blockSchema.fields.indices) {
        val dataType = blockSchema.fields(index).dataType
        if (dataType == StringType) {
          newVariableBaseOffset(index) =
            _UNSAFE.allocateMemory(variableLengthRecord(index))
          newVariableLengthRecord(index) = variableLengthRecord(index)
          copyMemory(
            null,
            variableBaseOffset(index),
            null,
            newVariableBaseOffset(index),
            variableLengthRecord(index)
          )
        } else {
          newVariableBaseOffset(index) = 0
          newVariableLengthRecord(index) = 0
        }
      }

      for (i <- size().toInt until (size().toInt + other.size().toInt)) {
        val otherInternalRow = otherIterator.next().copy()
        for (j <- 0 until variableNum) {
          val ordinal = indexOfVField(j)
          // copy the variable-length data from row to the variable-length area of block
          // we getString from the row and set it into the block variable-length area
          val targetStringBA =
            otherInternalRow.getString(ordinal).map(_.toByte).toArray
          val offset = newVariableLengthRecord(ordinal)
          val stringSize = targetStringBA.length
          newVariableLengthRecord(ordinal) += stringSize
          newVariableBaseOffset(ordinal) = _UNSAFE.reallocateMemory(
            newVariableBaseOffset(ordinal),
            newVariableLengthRecord(ordinal)
          )
          setString(
            newVariableBaseOffset(ordinal) + offset,
            targetStringBA,
            stringSize
          )
          // data has been copied, then we store long value in the field to           // data has been copied, then we store long value in the field to represent the offset and size the offset and size
          val offsetAndSize = offset << 32 + targetStringBA.length
          otherInternalRow.setLong(ordinal, offsetAndSize)
        }
        // copy the fixed-length data of the row to the fixed-length data area of block
        copyDataFromInternalRow(newBaseOffset, i, otherInternalRow)
      }
      val newBlockSize = blockSize + otherIterator.size
      new UnsafeInternalRowBlock(
        newBaseOffset,
        newVariableBaseOffset,
        newVariableLengthRecord,
        blockSchema,
        newBlockSize
      )
    }
  }

  /** trf: This is a quickSort that directly runs on heap-off memory
    * priority: it's an Array[Int], each element in it indicates a field whose value should be considered in sort
    * nothing will return, because we directly control the memory
    */
  private def quickSort(
      theBaseOffset: Long,
      priority: Array[Int],
      left: Int,
      right: Int
  ): Unit = {
    if (left < right) {
      val partitionIndex = partition(theBaseOffset, priority, left, right)
      quickSort(theBaseOffset, priority, left, partitionIndex - 1)
      quickSort(theBaseOffset, priority, partitionIndex + 1, right)
    }
  }

  private def partition(
      theBaseOffset: Long,
      priority: Array[Int],
      left: Int,
      right: Int
  ): Int = {
    val pivot = left
    var j =
      pivot + 1 // recorde the index of next row that is larger than the row gotten by pivot
    var i = j
    val firstDataType = blockSchema.fields(priority(0)).dataType
    val firstOrdinal = priority(0)
    firstDataType match {
      case DoubleType =>
        while (i < right) {
          val value_i = getDouble(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getDouble(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case FloatType =>
        while (i < right) {
          val value_i = getFloat(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getFloat(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case IntegerType =>
        while (i < right) {
          val value_i = getInt(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getInt(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case LongType =>
        while (i < right) {
          val value_i = getLong(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getLong(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case StringType =>
        while (i < right) {
          val value_i = getString(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getString(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case BooleanType =>
        while (i < right) {
          val value_i = getBoolean(theBaseOffset + i * rowSize, firstOrdinal)
          val value_pivot =
            getBoolean(theBaseOffset + pivot * rowSize, firstOrdinal)
          if (value_i < value_pivot) {
            swap(theBaseOffset, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(theBaseOffset, priority.drop(1), i, pivot)) {
              swap(theBaseOffset, i, j)
              j += 1
            }
          }
          i += 1
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    swap(theBaseOffset, pivot, j - 1)
    return j - 1
  }

  /** This method is used to compare two rows under multi-conditions
    */
  private def smallerThan(
      theBaseOffset: Long,
      priority: Array[Int],
      i: Int,
      j: Int,
      index: Int = 0
  ): Boolean = {
    if (index == priority.length) return false // comparison ends
    val ordinal = priority(index)
    val castType = schema().fields(ordinal).dataType
    castType match {
      case DoubleType =>
        val value_i = getDouble(theBaseOffset + i * rowSize, ordinal)
        val value_j = getDouble(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case FloatType =>
        val value_i = getFloat(theBaseOffset + i * rowSize, ordinal)
        val value_j = getFloat(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case IntegerType =>
        val value_i = getInt(theBaseOffset + i * rowSize, ordinal)
        val value_j = getInt(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case LongType =>
        val value_i = getLong(theBaseOffset + i * rowSize, ordinal)
        val value_j = getLong(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case StringType =>
        val value_i = getString(theBaseOffset + i * rowSize, ordinal)
        val value_j = getString(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case BooleanType =>
        val value_i = getBoolean(theBaseOffset + i * rowSize, ordinal)
        val value_j = getBoolean(theBaseOffset + j * rowSize, ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(theBaseOffset, priority, i, j, index + 1)
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    false
  }

  /** swap the row in the memory space whose baseOffset is defined by theBaseOffset
    *
    * @param theBaseOffset : the baseOffset of the memory space
    * @param i             : the row index
    * @param j             : the other row index
    */
  private def swap(theBaseOffset: Long, i: Int, j: Int): Unit = {
    val tmpAddress = _UNSAFE.allocateMemory(rowSize)
    copyMemory(null, theBaseOffset + j * rowSize, null, tmpAddress, rowSize)
    copyMemory(
      null,
      theBaseOffset + i * rowSize,
      null,
      theBaseOffset + j * rowSize,
      rowSize
    )
    copyMemory(null, tmpAddress, null, theBaseOffset + i * rowSize, rowSize)
    _UNSAFE.freeMemory(tmpAddress)
  }

  /** Copy data from an InternalRow to a InternalBlock whose baseOffset is defined by the BaseOffset
    *
    * @param theBaseOffset    : the baseOffset of the UnsafeInternalBlock that gets the data
    * @param i                : the ordinal indicates the place to store the copied data in the UnsafeInternalBlock
    * @param otherInternalRow : the InternalRow from which copy the data
    */
  private def copyDataFromInternalRow(
      theBaseOffset: Long,
      i: Int,
      otherInternalRow: InternalRow
  ): Unit = {
    copyDataFromRowToBlock(blockSchema, theBaseOffset, i, otherInternalRow)
  }

  /** Copy data from an UnsafeInternalRow to theBaseOffset of an UnsafeInternalBlock
    *
    * @param theBaseOffset    : the baseOffset of the UnsafeInternalBlock that gets the data
    * @param i                : the ordinal indicates the place to store the copied data in the UnsafeInternalBlock
    * @param otherInternalRow : the UnsafeInternalRow from which copy the data
    */
  private def copyDataFromUnsafeInternalRow(
      theBaseOffset: Long,
      i: Int,
      sourBaseOffset: Long,
      sourceIndex: Int
  ): Unit = {
    val destinationBaseOffset = theBaseOffset + i * rowSize
    copyMemory(
      null,
      sourBaseOffset + sourceIndex * rowSize,
      null,
      destinationBaseOffset,
      rowSize
    )
  }

  /** Get the Option[Seq[String] indicates the sort order
    *
    * @return the sort order
    */
  override def getDictionaryOrder: Option[Seq[String]] = {
    dictionaryOrder
  }

//  /** Let JVM do the GC for us
//    */
//  override def finalize(): Unit = {
//    _UNSAFE.freeMemory(baseOffset) // free the memory of fixed-length area
//    val variableFields = blockSchema.fields.filter(_.dataType == StringType)
//    for (field <- variableFields) {
//      // free the memory of the variable-length area
//      val index = blockSchema.fields.indexOf(field)
//      _UNSAFE.freeMemory(variableBaseOffset(index))
//    }
//    super.finalize()
//  }

}

object UnsafeInternalRowBlock extends UnsafeHelper {

  /** Initialize the [[UnsafeInternalRowBlock]] by array of [[InternalRow]] */
  def apply(
      rows: Array[InternalRow],
      schema: StructType
  ): UnsafeInternalRowBlock = {
    val variableFields = schema.fields.filter(_.dataType == StringType)
    val variableNum = variableFields.length
    val variableBaseOffset = new Array[Long](schema.fields.length)
    val variableLengthRecord = new Array[Long](schema.fields.length)
    val bitMapSize = calculateBitMapWidthInWords(schema.fields.length) * 8
    val rowSize = bitMapSize + schema.fields.length * fieldByteSize

    // set the baseOffset for fixed-length area
    val blockBaseOffset: Long = _UNSAFE.allocateMemory(rows.length * rowSize)
    val indexOfVField = new Array[Int](variableNum) // index of variable Field
    for (index <- 0 until variableNum) {
      indexOfVField(index) = schema.fields.indexOf(variableFields(index))
    }

    // get the size of each variable-length area
    for (row <- rows) {
      // val rowBaseOffset = row.asInstanceOf[UnsafeInternalRow].baseOffset
      for (index <- indexOfVField) {
        val vStr = row.getString(index)
        val dataByteArray = vStr.map(_.toByte).toArray
        variableLengthRecord(index) += dataByteArray.length
      }
    }

    // allocate memory for variable-length area and set its baseOffset
    for (structField <- schema.fields) {
      val index = schema.fields.indexOf(structField)
      if (structField.dataType == StringType) {
        variableBaseOffset(index) =
          _UNSAFE.allocateMemory(variableLengthRecord(index))
      } else {
        variableBaseOffset(index) = 0
      }
    }

    // alter the relative offset stored in variable-filed, it should be recalculated according to the variableBaseOffset of block
    val offsetRecord = new Array[Long](schema.fields.length)
    var rowCursor = 0
    for (row <- rows) {
      // val unsafeInternalRow = row.asInstanceOf[UnsafeInternalRow]
      for (index <- 0 until variableNum) {
        val indexInRow = indexOfVField(index)
        val vStr = row.getString(indexInRow)
        if (vStr != null) {
          val dataByteArray = vStr.map(_.toByte).toArray
          val size: Int = dataByteArray.length
          setString(
            variableBaseOffset(indexInRow) + offsetRecord(indexInRow),
            dataByteArray,
            size
          ) // set the data into variable-data are
          val offsetAndSize: Long = (offsetRecord(indexInRow) << 32) + size
          row.setLong(indexInRow, offsetAndSize) // set new value
          offsetRecord(indexInRow) += size
        }
      }
      copyDataFromRowToBlock(schema, blockBaseOffset, rowCursor, row)
      rowCursor += 1
    }
    new UnsafeInternalRowBlock(
      blockBaseOffset,
      variableBaseOffset,
      variableLengthRecord,
      schema,
      rows.length
    )
  }

  /** Return the builder for building [[UnsafeInternalRowBlock]] */
  def builder(schema: StructType): UnsafeInternalRowBlockBuilder =
    new UnsafeInternalRowBlockBuilder(schema)
}

class UnsafeInternalRowBlockBuilder(schema: StructType)
    extends InternalBlockBuilder
    with UnsafeHelper {
  private var rows: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()

  override def add(row: InternalRow): Unit = rows.append(row.copy())

  override def build(): UnsafeInternalRowBlock = {
    UnsafeInternalRowBlock.apply(rows.toArray, schema)
  }
}

trait UnsafeHelper {
  private[block] val _UNSAFE: Unsafe = getUnsafe
  private[block] val UNSAFE_COPY_THRESHOLD = 1024L * 1024
  private[block] val fieldByteSize: Int = 8
  private[block] val BYTE_ARRAY_OFFSET =
    _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])

  private def getUnsafe: Unsafe = {
    val f = classOf[Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    f.get(null).asInstanceOf[Unsafe]
  }

  private[storage] def calculateBitMapWidthInWords(numFields: Int): Int =
    (numFields + 63) / 64

  private[storage] def copyMemory(
      src: AnyRef,
      srcOffset: Long,
      dst: AnyRef,
      dstOffset: Long,
      length: Long
  ): Unit = {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, length)
  }

  def setString(
      variableBaseAddress: Long,
      value: Array[Byte],
      stringSize: Long
  ): Unit = {
    copyMemory(value, BYTE_ARRAY_OFFSET, null, variableBaseAddress, stringSize)

  }

  def copyDataFromRowToBlock(
      schema: StructType,
      baseOffset: Long,
      i: Int,
      otherInternalRow: InternalRow
  ): Unit = {
    val fields = schema.fields
    val bitMapSize = calculateBitMapWidthInWords(schema.fields.length) * 8
    val rowSize = schema.fields.length * fieldByteSize + bitMapSize
    val destinationBaseOffset = baseOffset + i * rowSize + bitMapSize
    val bitMapData = Utils.calculateBitMapForRow(schema, otherInternalRow)
    for (i <- bitMapData.indices)
      _UNSAFE.putLong(
        null,
        destinationBaseOffset + i * 8 - bitMapSize,
        bitMapData(i)
      )
    for (ordinal <- fields.indices) {
      val dataType = fields(ordinal).dataType
      dataType match {
        case DoubleType =>
          _UNSAFE.putDouble(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getDouble(ordinal)
          )
        case FloatType =>
          _UNSAFE.putFloat(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getFloat(ordinal)
          )
        case IntegerType =>
          _UNSAFE.putInt(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getInt(ordinal)
          )
        case LongType =>
          _UNSAFE.putLong(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getLong(ordinal)
          )
        case StringType =>
          _UNSAFE.putLong(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getLong(ordinal)
          )
        case BooleanType =>
          _UNSAFE.putBooleanVolatile(
            null,
            destinationBaseOffset + ordinal * fieldByteSize,
            otherInternalRow.getBoolean(ordinal)
          )
        case _ =>
          throw new Exception("find dataType that is not allowed during swap!")
      }
    }
  }
}
