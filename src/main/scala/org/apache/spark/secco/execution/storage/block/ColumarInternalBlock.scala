package org.apache.spark.secco.execution.storage.block
import org.apache.spark.{Partitioner, SPARK_BRANCH, SPARK_VERSION}
import org.apache.spark.secco.types._
import org.apache.spark.secco.execution.storage.row._
import org.apache.spark.secco.execution.storage.Utils

import scala.collection.AbstractIterator
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/** The InternalBlock backed by columnar storage */
class ColumnarInternalBlock(
                             structType: StructType,
                             private[block] var bitMapAddress: Long,
                             private[block] var columnAddresses: Array[Long],
                             private[block] var variableLengthZoneAddress: Long
                           ) extends InternalBlock with ColumnLike with RowLike {

  //  private[block] var structType: StructType = _
  private[block] var rowNum: Int = _
  private var iterIndex: Int = 0

  //  private var baseObject = null
  //  private[block] var columnAddresses: Array[Long] = _

  //  private[block] var variableLengthZoneAddress: Long = _
  private[block] var variableLengthZoneSize: Int = _
  private[block] var currentUsedSizeInVariableLengthZone: Int = _

  //  private[block] var bitMapAddress: Long = _
  private[block] var bitMapSizeInBytes: Long = Utils.calculateBitMapWidthInBytes(structType.length)

  private[block] var dictionaryOrder: Option[Seq[String]] = None

  private val isString: Array[Boolean] = structType.map(item => item.dataType == StringType).toArray
  private val sizeMapArray: Array[Byte] = structType.map {
    case StructField(_, IntegerType, _) => 4.toByte
    case StructField(_, FloatType, _) => 4.toByte
    case StructField(_, BooleanType, _) => 1.toByte
    case StructField(_, LongType, _) => 8.toByte
    case StructField(_, DoubleType, _) => 8.toByte
    case StructField(_, StringType, _) => 8.toByte
    case _ => 8.toByte
  }.toArray

  override def getRow(rowIndex: Int): InternalRow = {
    val stringRecordArrayBuffer = new ArrayBuffer[Long]
    for(j <- structType.indices) if(isString(j)) stringRecordArrayBuffer.append(getLong(rowIndex, j))
    val variableLengthSize = stringRecordArrayBuffer.map(_.toInt).sum

    val numFields = structType.length
    val rowSize = Utils.calculateBitMapWidthInBytes(numFields) + numFields * 8 + variableLengthSize
    val row = new UnsafeInternalRow(numFields, rowSize)
    row.copyBitMapFrom(null, bitMapAddress + rowIndex * bitMapSizeInBytes)

    for(j <- columnAddresses.indices) {
      if(!isString(j)) {
        val elemSize = sizeMapArray(j)
//        row.copyDataFromWithDataSize(null, columnAddresses(j) + rowIndex * elemSize, j, elemSize)
        row.copyDataFromWithDataSize(null, columnAddresses(j) + rowIndex * elemSize, elemSize, j)
      } else {
        val recordAddress = columnAddresses(j) + rowIndex * 8
        val offsetAndSize = Utils._UNSAFE.getLong(null, recordAddress)
        row.copyStringFrom(variableLengthZoneAddress, offsetAndSize, j)
      }
    }
    row
  }

  private def getString(i: Int, j: Int): String =
    Utils.getStringAtColumnAddress(columnAddresses(j), variableLengthZoneAddress, i, 8)

  private def getLong(i: Int, j: Int): Long =
    Utils._UNSAFE.getLong(columnAddresses(j) + i * 8)

  override def getDictionaryOrder: Option[Seq[String]] = dictionaryOrder

  override def iterator: Iterator[InternalRow] = new AbstractIterator[InternalRow] {
    override def hasNext: Boolean = iterIndex < rowNum

    override def next(): InternalRow = { val row = getRow(iterIndex); iterIndex += 1; row}
  }

  override def size(): Long = rowNum.toLong

  override def isEmpty(): Boolean = rowNum == 0

  override def nonEmpty(): Boolean = rowNum != 0

  override def schema(): StructType = structType

  private def smaller(i: Int, j: Int, columnAddresses_a:Array[Long], columnAddresses_b:Array[Long],
                      DictionaryOrder: Seq[String]): Boolean = {
    var compareResult: Int = 0
    for (item <- DictionaryOrder) {
      val columnId = structType.getFieldIndex(item).get
      val address_a = columnAddresses_a(columnId)
      val address_b = columnAddresses_b(columnId)
      val dataType = structType(item).dataType
      val elemSize = Utils.sizeMap(dataType)
      structType(item).dataType match{
        case BooleanType =>
          val (a,b) = (Utils._UNSAFE.getBooleanVolatile(null, address_a + i * elemSize),
            Utils._UNSAFE.getBooleanVolatile(null, address_b + j * elemSize))
          compareResult = Utils.compare(a,b)
        case IntegerType =>
          val (a ,b) = (Utils._UNSAFE.getInt(null, address_a + i * elemSize),
            Utils._UNSAFE.getInt(null, address_b + j * elemSize))
          compareResult = Utils.compare(a,b)
        case LongType =>
          val (a, b) = (Utils._UNSAFE.getLong(null, address_a + i * elemSize),
            Utils._UNSAFE.getLong(null, address_b + j * elemSize))
          compareResult = Utils.compare(a,b)
        case FloatType =>
          val (a, b) = (Utils._UNSAFE.getFloat(null, address_a + i * elemSize),
            Utils._UNSAFE.getFloat(null, address_b + j * elemSize))
          compareResult = Utils.compare(a, b)
        case DoubleType =>
          val (a, b) = (Utils._UNSAFE.getDouble(null, address_a + i * elemSize),
            Utils._UNSAFE.getDouble(null, address_b + j * elemSize))
          compareResult = Utils.compare(a, b)
        case StringType => val (a, b) = (Utils.getStringAtColumnAddress(address_a, variableLengthZoneAddress, i, 8),
          Utils.getStringAtColumnAddress(address_b, variableLengthZoneAddress, j, 8))
          compareResult = Utils.compare(a, b)
        case _ => throw new NotImplementedError("Unhandled DataType encountered")
      }
      if(compareResult == 1) return true
      else if(compareResult == -1) return false
    }
    return false
  }

  private def swap_cascade(i: Int, j: Int): Unit = {
    for(idx <- structType.indices){
      val elemSize = sizeMapArray(j)
      val temp = Utils._UNSAFE.allocateMemory(elemSize)
      val (address_i, address_j) = (columnAddresses(idx) + i * elemSize, columnAddresses(idx) + j * elemSize)
      Utils.swapMemory(address_i, address_j, temp, elemSize)
      Utils._UNSAFE.freeMemory(temp)
    }
  }

  private def swap_baseline(i: Int, j: Int, baselineAddress: Long): Unit = {
    val temp = Utils._UNSAFE.allocateMemory(4)
    val (address_i, address_j) = (baselineAddress + i * 4, baselineAddress + j * 4)
    Utils.swapMemory(address_i, address_j, temp, 4)
    Utils._UNSAFE.freeMemory(temp)
  }

  private def heap_sort(DictionaryOrder: Seq[String], in_place: Boolean = false): InternalBlock = {

    lazy val baselineAddress: Long = Utils._UNSAFE.allocateMemory(4 * rowNum)
    if(!in_place) for(i <- 0 until rowNum) Utils._UNSAFE.putInt(null, baselineAddress + i * 4, i)

    def swap(i: Int, j: Int): Unit = if(in_place) swap_cascade(i, j) else swap_baseline(i, j, baselineAddress)

    def sift_down(i: Int, pseudo_length: Int): Unit = {
      var father = i
      var son = 2 * father + 1
      if (son >= pseudo_length) return
      while(son < pseudo_length){
        if((son + 1 < pseudo_length) && smaller(i, i+1, this.columnAddresses, this.columnAddresses, DictionaryOrder))
          son = son + 1
        if( ! smaller(father, son, this.columnAddresses, this.columnAddresses, DictionaryOrder)) return
        else swap(father, son)
      }
    }

    for(i <- rowNum / 2 - 1 to 0) sift_down(i, rowNum)
    for(i <- rowNum - 1 to 1){
      swap(0, i)
      sift_down(0, i)
    }

    if(!in_place) {
      val structType = this.structType
      val columnAddresses = {
        val arr = new Array[Long](structType.length)
        for (j <- arr.indices) {
          val elemSize = sizeMapArray(j)
          arr(j) = Utils._UNSAFE.allocateMemory(rowNum * elemSize)
        }
        arr
      }
      val bitMapAddress = Utils._UNSAFE.allocateMemory(999)
      val variableLengthZoneAddress =
        Utils._UNSAFE.allocateMemory(Utils.roundNumberOfBytesToNearestWord(currentUsedSizeInVariableLengthZone))
      val block = new ColumnarInternalBlock(structType, bitMapAddress, columnAddresses, variableLengthZoneAddress)
      block.rowNum = this.rowNum
      block.bitMapSizeInBytes = this.bitMapSizeInBytes
      block.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone
      for(i <- 0 until rowNum) {
        val temp_idx = Utils._UNSAFE.getInt(null, baselineAddress + i * 4)
        for(j <- columnAddresses.indices) {
          val elemSize = sizeMapArray(j)
          Utils.copyMemory(null, this.columnAddresses(j) + temp_idx * elemSize,
            null, block.columnAddresses(j) + i * elemSize, elemSize)
        }
        Utils.copyMemory(null, this.bitMapAddress + temp_idx * bitMapSizeInBytes,
          null, block.bitMapAddress + i * bitMapSizeInBytes, bitMapSizeInBytes)
        Utils.copyMemory(null, variableLengthZoneAddress, null,
          block.variableLengthZoneAddress, currentUsedSizeInVariableLengthZone)
      }
      block.iterIndex = 0
      block.dictionaryOrder = dictionaryOrder
      block
    }else {
      this.dictionaryOrder = dictionaryOrder
      return this
    }
  }

  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock = heap_sort(DictionaryOrder)

  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  private def isNull(i: Int, j: Int): Boolean = {
    val mask = 1L << (j & 0x3f)  // mod 64 and shift
    val wordAddress = bitMapAddress + i * bitMapSizeInBytes + (j >> 6) * 8
    val word = Utils._UNSAFE.getLong(null, wordAddress)
    return (mask & word) != 0L
  }

  override def show(num: Int): Unit = {
    print("row_index")
    structType.foreach(item => print(s"\t${item.name}"))
    print("\n")
    for(i <- 0 until rowNum){
      print(i)
      for(j <- structType.indices){
        if(isNull(i,j)) print("\tNull")
        else {
          val elemSize = sizeMapArray(j)
          val elem = structType(j).dataType match {
            case BooleanType => Utils._UNSAFE.getBoolean(null, columnAddresses(j) + i * elemSize)
            case IntegerType => Utils._UNSAFE.getInt(null, columnAddresses(j) + i * elemSize)
            case LongType => Utils._UNSAFE.getLong(null, columnAddresses(j) + i * elemSize)
            case FloatType => Utils._UNSAFE.getFloat(null, columnAddresses(j) + i * elemSize)
            case DoubleType => Utils._UNSAFE.getDouble(null, columnAddresses(j) + i * elemSize)
            case StringType => "\"" + getString(i, j) + "\""
          }
          print(s"\t${elem}")
        }
      }
      print("\n")
    }
  }

  override def toArray(): Array[InternalRow] = {
    val rowArray = new Array[InternalRow](rowNum)
    for(i <- rowArray.indices) rowArray(i) = getRow(i)
    return rowArray
  }

  override def getColumnByOrdinal[T: ClassTag](j: Int): Array[T] = {
    val array = new Array[T](rowNum)   // lgh: Problem solved, over.
    if(!isString(j))
      Utils.copyMemory(null, columnAddresses(j), array, Utils._UNSAFE.arrayBaseOffset(classOf[Array[T]]),
        rowNum * sizeMapArray(j))
    else
      for(idx <- 0 until rowNum) array(idx) = getString(idx, j).asInstanceOf[T]
    return array
  }

  override def getColumn[T: ClassTag](columnName: String): Array[T] = {
    val columnId = structType.getFieldIndex(columnName).get
    return getColumnByOrdinal(columnId)
  }

  private def setString(i:Int, j:Int, str: String):Unit = {
    val dataByteArray = str.map(_.toByte).toArray
    val stringSize = dataByteArray.length
    val stringOffset = currentUsedSizeInVariableLengthZone
    if(currentUsedSizeInVariableLengthZone + stringSize > variableLengthZoneSize){
      val (address, size) = Utils.grow(variableLengthZoneAddress, currentUsedSizeInVariableLengthZone, stringSize)
      variableLengthZoneAddress = address
      variableLengthZoneSize = size
    }
    Utils.copyMemory(dataByteArray, Utils.BYTE_ARRAY_OFFSET,
      null, variableLengthZoneAddress + stringOffset, stringSize.toLong)
    val offsetAndSize = stringOffset.toLong << 32 | stringSize.toLong
    Utils._UNSAFE.putLong(null, columnAddresses(j) + i * 8, offsetAndSize)
    currentUsedSizeInVariableLengthZone += stringSize
  }

  private def set(i:Int, j:Int, value:Any, dataType:DataType):Unit = {
    val elemSize = Utils.sizeMap(dataType)
    dataType match{
      case BooleanType =>
        Utils._UNSAFE.putBoolean(null, columnAddresses(j) + i * elemSize, value.asInstanceOf[Boolean])
      case IntegerType =>
        Utils._UNSAFE.putInt(null, columnAddresses(j) + i * elemSize, value.asInstanceOf[Int])
      case LongType =>
        Utils._UNSAFE.putLong(null, columnAddresses(j) + i * elemSize, value.asInstanceOf[Long])
      case FloatType =>
        Utils._UNSAFE.putFloat(null, columnAddresses(j) + i * elemSize, value.asInstanceOf[Float])
      case DoubleType =>
        Utils._UNSAFE.putDouble(null, columnAddresses(j) + i * elemSize, value.asInstanceOf[Double])
      case StringType => setString(i, j, value.asInstanceOf[String])
    }
  }

  override def merge(other: InternalBlock,
                     maintainSortOrder: Boolean): InternalBlock = {

    class SchemaNotConsistentException(message: String) extends IllegalArgumentException(message:String)
    if(!this.schema().equals(other.schema()))
      throw new SchemaNotConsistentException(s"Schemas of the two blocks to be merged are not consistent.\n" +
      s"This: ${this.schema()}\n" +
      s"Other: ${other.schema()}")
    if(maintainSortOrder && (this.dictionaryOrder.isEmpty || other.getDictionaryOrder.isEmpty ||
      !this.dictionaryOrder.get.equals(other.getDictionaryOrder.get)))
      throw new IllegalArgumentException("DictionaryOrders not existing or not consistent.")

    //    val blockBuilder =
    //      new ColumnarInternalBlockBuilder(this.schema(), this.rowNum + other.size().toInt, variableLengthZoneSize: Int)
    //    blockBuilder.rowNum = this.rowNum + other.size().toInt
    //    blockBuilder.structType = this.structType

    //    for(j <- blockBuilder.columnAddresses.indices) {
    //      val elemSize = Utils.sizeMap(structType(j).dataType)
    //      blockBuilder.columnAddresses(j) = Utils._UNSAFE.allocateMemory(blockBuilder.rowNum * elemSize)
    //    }

    //    blockBuilder.bitMapAddress = Utils._UNSAFE.allocateMemory(blockBuilder.rowNum * bitMapSizeInBytes)
    //    blockBuilder.bitMapSizeInBytes = this.bitMapSizeInBytes.toInt

    (other, maintainSortOrder) match {
      case (otherColumnar: ColumnarInternalBlock, false) =>

        val blockBuilder = new ColumnarInternalBlockBuilder(this.schema(), this.rowNum + other.size().toInt,
          this.currentUsedSizeInVariableLengthZone + otherColumnar.currentUsedSizeInVariableLengthZone)
        Utils.copyMemory(null, this.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)
        Utils.copyMemory(null, otherColumnar.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress + this.currentUsedSizeInVariableLengthZone,
          otherColumnar.currentUsedSizeInVariableLengthZone)
        blockBuilder.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone +
          otherColumnar.currentUsedSizeInVariableLengthZone
        blockBuilder.variableLengthZoneSize = blockBuilder.currentUsedSizeInVariableLengthZone

        for(j <- columnAddresses.indices) {
          val elemSize = Utils.sizeMap(structType(j).dataType)
          Utils.copyMemory(null, this.columnAddresses(j),
            null, blockBuilder.columnAddresses(j), this.rowNum * elemSize)
          if(structType(j).dataType != StringType){
            Utils.copyMemory(null, otherColumnar.columnAddresses(j),
              null, blockBuilder.columnAddresses(j) + this.rowNum * elemSize, otherColumnar.rowNum * elemSize)
          }
          else{
            for(i <- 0 until otherColumnar.rowNum){
              val originalOffsetAndSize = Utils._UNSAFE.getLong(null, otherColumnar.columnAddresses(j) + i * 8)
              val offsetAndSize = (originalOffsetAndSize >>> 32 + this.currentUsedSizeInVariableLengthZone) << 32 |
                originalOffsetAndSize.toInt.toLong
              Utils._UNSAFE.putLong(null, blockBuilder.columnAddresses(j) + i * 8, offsetAndSize)
            }
          }
        }

        blockBuilder.rowNum = this.rowNum + other.size().toInt
        Utils.copyMemory(null, this.bitMapAddress,
          null, blockBuilder.bitMapAddress, this.rowNum * bitMapSizeInBytes)
        Utils.copyMemory(null, otherColumnar.bitMapAddress,
          null, blockBuilder.bitMapAddress + this.rowNum * bitMapSizeInBytes,
          otherColumnar.rowNum * bitMapSizeInBytes)

        return blockBuilder.build()

      case (otherColumnar: ColumnarInternalBlock, true) =>

        val blockBuilder = new ColumnarInternalBlockBuilder(this.schema(), this.rowNum + other.size().toInt,
          this.currentUsedSizeInVariableLengthZone + otherColumnar.currentUsedSizeInVariableLengthZone)
        Utils.copyMemory(null, this.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)
        Utils.copyMemory(null, otherColumnar.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress + this.currentUsedSizeInVariableLengthZone,
          otherColumnar.currentUsedSizeInVariableLengthZone)
        blockBuilder.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone +
          otherColumnar.currentUsedSizeInVariableLengthZone
        blockBuilder.variableLengthZoneSize = blockBuilder.currentUsedSizeInVariableLengthZone

        val selectArray = new Array[Boolean](blockBuilder.rowNum)
        val idxArray = new Array[Int](blockBuilder.rowNum)
        var (idx_i, idx_j, idx) = (0, 0, 0)
        if(smaller(idx_i, idx_j, this.columnAddresses, otherColumnar.columnAddresses, this.dictionaryOrder.get)){
          selectArray(idx) = true
          idxArray(idx) = idx_i
          idx_i += 1
          idx += 1
        }
        else{
          selectArray(idx) = false
          idxArray(idx) = idx_j
          idx_j += 1
          idx += 1
        }

        for (j <- structType.indices){
          val dataType = structType(j).dataType
          val elemSize = Utils.sizeMap(dataType)
          for(i <- 0 until blockBuilder.rowNum){
            val address = if(selectArray(i)) this.columnAddresses(j) + idxArray(i) * elemSize
            else otherColumnar.columnAddresses(j) + idxArray(i) * elemSize
            if (!(dataType == StringType && !selectArray(i)))
              Utils.copyMemory(null, address, null, blockBuilder.columnAddresses(j) + i * elemSize, elemSize)
            else{
              val originalOffsetAndSize = Utils._UNSAFE.getLong(null, otherColumnar.columnAddresses(j) + i * 8)
              val offsetAndSize = (originalOffsetAndSize >>> 32 + this.currentUsedSizeInVariableLengthZone) << 32 |
                originalOffsetAndSize.toInt.toLong
              Utils._UNSAFE.putLong(null, blockBuilder.variableLengthZoneAddress + i * 8, offsetAndSize)
            }
          }
        }

        blockBuilder.rowNum = this.rowNum + other.size().toInt
        for(i <- 0 until blockBuilder.rowNum){
          val address = if(selectArray(i)) this.bitMapAddress + idxArray(i) * bitMapSizeInBytes
          else otherColumnar.bitMapAddress + idxArray(i) * bitMapSizeInBytes
          Utils.copyMemory(null, address,
            null, blockBuilder.bitMapAddress + i * bitMapSizeInBytes, bitMapSizeInBytes)
        }

        blockBuilder.dictionaryOrder = this.dictionaryOrder

        return blockBuilder.build()
      case (_, false) =>

        val rowArrayOfOther: Array[InternalRow] = other.toArray()

        val blockBuilder = new ColumnarInternalBlockBuilder(this.schema(), this.rowNum + other.size().toInt,
          2 * this.currentUsedSizeInVariableLengthZone)
        Utils.copyMemory(null, this.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)
        blockBuilder.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone

        for(j <- columnAddresses.indices) {
          val dataType = structType(j).dataType
          val elemSize = Utils.sizeMap(dataType)
          Utils.copyMemory(null, this.columnAddresses(j),
            null, blockBuilder.columnAddresses(j), this.rowNum * elemSize)
          //          for(i <- this.rowNum until blockBuilder.rowNum){
          //            val value = rowArrayOfOther(i).get(j, dataType)
          //            blockBuilder.set(i, j, value, dataType)
          //          }
        }

        Utils.copyMemory(null, this.bitMapAddress,
          null, blockBuilder.bitMapAddress, this.rowNum * bitMapSizeInBytes)

        blockBuilder.rowNum = this.rowNum

        rowArrayOfOther.foreach(item => blockBuilder.add(item))

        return blockBuilder.build()

      case (_, true) =>

        val blockBuilder = new ColumnarInternalBlockBuilder(this.schema(), this.rowNum + other.size().toInt,
          2 * this.currentUsedSizeInVariableLengthZone)
        val rowArrayOfOther: Array[InternalRow] = other.toArray()

        blockBuilder.variableLengthZoneAddress =
          Utils._UNSAFE.allocateMemory(
            Utils.roundNumberOfBytesToNearestWord(2 * this.currentUsedSizeInVariableLengthZone))
        Utils.copyMemory(null, this.variableLengthZoneAddress,
          null, blockBuilder.variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)

        def smaller_(i: Int, j: Int): Boolean = {
          var compareResult: Int = 0
          for (item <- this.dictionaryOrder.get) {
            val columnId = structType.getFieldIndex(item).get
            val address_a = this.columnAddresses(columnId)
            structType(item).dataType match {
              case BooleanType =>
                val (a, b) = (Utils._UNSAFE.getBooleanVolatile(null, address_a + i * Utils.sizeMap(BooleanType)),
                  rowArrayOfOther(i).get(j, BooleanType).asInstanceOf[Boolean])
                compareResult = Utils.compare(a, b)
              case IntegerType =>
                val (a, b) = (Utils._UNSAFE.getInt(null, address_a + i * Utils.sizeMap(IntegerType)),
                  rowArrayOfOther(i).get(j, IntegerType).asInstanceOf[Int])
                compareResult = Utils.compare(a, b)
              case LongType =>
                val (a, b) = (Utils._UNSAFE.getLong(null, address_a + i * Utils.sizeMap(LongType)),
                  rowArrayOfOther(i).get(j, LongType).asInstanceOf[Long])
                compareResult = Utils.compare(a, b)
              case FloatType =>
                val (a, b) = (Utils._UNSAFE.getFloat(null, address_a + i * Utils.sizeMap(FloatType)),
                  rowArrayOfOther(i).get(j, FloatType).asInstanceOf[Float])
                compareResult = Utils.compare(a, b)
              case DoubleType =>
                val (a, b) = (Utils._UNSAFE.getDouble(null, address_a + i * Utils.sizeMap(DoubleType)),
                  rowArrayOfOther(i).get(j, DoubleType).asInstanceOf[Double])
                compareResult = Utils.compare(a, b)
              case StringType => val (a, b) =
                (Utils.getStringAtColumnAddress(address_a, variableLengthZoneAddress, i, 8),
                  rowArrayOfOther(i).get(j, StringType).asInstanceOf[String])
                compareResult = Utils.compare(a, b)
              case _ => throw new NotImplementedError("Unhandled DataType encountered")
            }
            if (compareResult == 1) return true
            else if (compareResult == -1) return false
          }
          return false
        }

        val selectArray = new Array[Boolean](blockBuilder.rowNum)
        val idxArray = new Array[Int](blockBuilder.rowNum)
        var (idx_i, idx_j, idx) = (0, 0, 0)
        if(smaller_(idx_i, idx_j)){
          selectArray(idx) = true
          idxArray(idx) = idx_i
          idx_i += 1
          idx += 1
        }
        else{
          selectArray(idx) = false
          idxArray(idx) = idx_j
          idx_j += 1
          idx += 1
        }

        for (j <- structType.indices){
          val dataType = structType(j).dataType
          val elemSize = Utils.sizeMap(dataType)
          for(i <- 0 until blockBuilder.rowNum){
            if(selectArray(i))
              Utils.copyMemory(null, this.columnAddresses(j) + idxArray(i) * elemSize,
                null, blockBuilder.columnAddresses(j) + i * elemSize, elemSize)
            else{
              val value = rowArrayOfOther(i).get(j, dataType)
              blockBuilder.set(i, j, value, dataType)
            }
          }
        }

        val bitMapSizeInWords = (bitMapSizeInBytes.toInt + 7) / 8
        for(i <- 0 until blockBuilder.rowNum){
          if(selectArray(i)) {
            Utils.copyMemory(null, this.bitMapAddress + idxArray(i) * bitMapSizeInBytes,
              null, blockBuilder.bitMapAddress + i * bitMapSizeInBytes, bitMapSizeInBytes)
          } else{
            val row = rowArrayOfOther(i)
            val bitMaps = new Array[Long](bitMapSizeInWords)
            for(k <- bitMaps.indices) bitMaps(k) = 0L
            for(j <- structType.indices) {
              if (row.isNullAt(j)){
                val mask = 1L << (j & 0x3f)
                val wordOffset = j >> 6
                bitMaps(wordOffset) |= mask
              }
            }
            Utils.copyMemory(bitMaps, Utils._UNSAFE.arrayBaseOffset(classOf[Array[Long]]),
              null, blockBuilder.bitMapAddress + i * bitMapSizeInBytes, bitMapSizeInBytes)
          }
        }

        blockBuilder.dictionaryOrder = this.dictionaryOrder

        return blockBuilder.build()
    }

    //    return blockBuilder.build()
  }

  override def finalize(): Unit = {
    Utils._UNSAFE.freeMemory(bitMapAddress)
    Utils._UNSAFE.freeMemory(variableLengthZoneAddress)
    columnAddresses.foreach(item => Utils._UNSAFE.freeMemory(item))
    super.finalize()
  }
}

object ColumnarInternalBlock {

  /** Initialize the [[ColumnarInternalBlock]] by array of [[InternalRow]] */
  def apply(rows: Array[InternalRow], schema: StructType, isSorted: Boolean = false): ColumnarInternalBlock = {

    val row_num = rows.length

    val numFields = schema.length
    val bitMapSizeInWords = Utils.calculateBitMapWidthInWords(numFields)
    val bitMapAddress = Utils._UNSAFE.allocateMemory(row_num * bitMapSizeInWords * 8)

    val columnAddresses = {
      val arr = new Array[Long](schema.length)
      for(j <- arr.indices) {
        val elemSize = Utils.sizeMap(schema(j).dataType)
        arr(j) = Utils._UNSAFE.allocateMemory(
          Utils.roundNumberOfBytesToNearestWord((row_num * elemSize).toInt)                //TODO
        )
      }
      arr
    }

    val initialVariableLengthZoneSize = 0
    val variableLengthZoneAddress = Utils._UNSAFE.allocateMemory(initialVariableLengthZoneSize)

    val block = new ColumnarInternalBlock(schema, bitMapAddress, columnAddresses, variableLengthZoneAddress)
    block.rowNum = row_num
    block.variableLengthZoneSize = initialVariableLengthZoneSize


    for(j <- schema.indices) {
      val dataType = schema(j).dataType
      for(i <- 0 until block.rowNum){
        val value = rows(i).get(j, dataType)
        block.set(i, j, value, dataType)
      }
    }

    for(i <- 0 until block.rowNum){
      val row = rows(i)
      val bitMaps = new Array[Long](bitMapSizeInWords)
      for(k <- bitMaps.indices) bitMaps(k) = 0L
      for(j <- schema.indices) {
        if (row.isNullAt(j)){
          val mask = 1L << (j & 0x3f)
          val wordOffset = j >> 6
          bitMaps(wordOffset) |= mask
        }
      }
      Utils.copyMemory(bitMaps, Utils._UNSAFE.arrayBaseOffset(classOf[Array[Long]]),
        null, block.bitMapAddress + i * block.bitMapSizeInBytes, block.bitMapSizeInBytes)
    }

    if(isSorted){
      block.dictionaryOrder = Some(schema.map(_.name))
    }

    return block
  }

  /** Return the builder for building [[ColumnarInternalBlock]] */
  def builder(schema: StructType): ColumnarInternalBlockBuilder = {
    new ColumnarInternalBlockBuilder(schema)
  }
}

class ColumnarInternalBlockBuilder(private val schema: StructType) extends InternalBlockBuilder() {

  def this(schema: StructType, allocatedRowNum: Int, variableLengthZoneSize: Int){
    this(schema)
    Utils._UNSAFE.freeMemory(this.bitMapAddress)
    Utils._UNSAFE.freeMemory(this.variableLengthZoneAddress)
    this.columnAddresses.foreach(item => Utils._UNSAFE.freeMemory(item))

    this.allocatedRowNum = allocatedRowNum
    for(j <- this.columnAddresses.indices) this.columnAddresses(j) = Utils._UNSAFE.allocateMemory(columnSizes(j))

    this.variableLengthZoneAddress = Utils._UNSAFE.allocateMemory(variableLengthZoneSize)
    this.variableLengthZoneSize = variableLengthZoneSize
    this.bitMapAddress = Utils._UNSAFE.allocateMemory(this.bitMapZoneSize)
  }

  private[block] var dictionaryOrder: Option[Seq[String]] = None

  private[block] var rowNum: Int = 0

  private[block] var allocatedRowNum = 8

  //  private var baseObject = null
  private[block] def columnSizes: Array[Int] = {
    val arr = new Array[Int](schema.length)
    for(j <- arr.indices) arr(j) = allocatedRowNum * Utils.sizeMap(schema(j).dataType).toInt
    arr
  }

  private[block] val columnAddresses: Array[Long] = {
    val arr = new Array[Long](schema.length)
    for(j <- arr.indices) arr(j) = Utils._UNSAFE.allocateMemory(columnSizes(j))
    arr
  }

  private[block] val initialVariableLengthZoneSize = 8 * schema.length
  private[block] var variableLengthZoneAddress: Long = Utils._UNSAFE.allocateMemory(initialVariableLengthZoneSize)
  private[block] var variableLengthZoneSize: Int = initialVariableLengthZoneSize
  private[block] var currentUsedSizeInVariableLengthZone: Int = 0

  private[block] val bitMapSizeInWords: Int = Utils.calculateBitMapWidthInWords(schema.length)
  private[block] val bitMapSizeInBytes = 8 * bitMapSizeInWords
  private[block] def bitMapZoneSize: Int = allocatedRowNum * bitMapSizeInBytes
  private[block] var bitMapAddress: Long = Utils._UNSAFE.allocateMemory(bitMapZoneSize)

  private def growColumns(newlyAddedRowNum: Int): Unit ={
    if(rowNum + newlyAddedRowNum > allocatedRowNum) {
      for(j <- schema.indices){
        val dataType = schema(j).dataType
        val elemSize = Utils.sizeMap(dataType).toInt
        val (newAddress, _) =
          Utils.grow(columnAddresses(j), rowNum * elemSize, newlyAddedRowNum * elemSize)
        this.columnAddresses(j) = newAddress
      }
      val (newBitMapAddress, _) =
        Utils.grow(bitMapAddress, rowNum * bitMapSizeInBytes, newlyAddedRowNum * bitMapSizeInBytes)
      this.bitMapAddress = newBitMapAddress
      this.allocatedRowNum = rowNum + newlyAddedRowNum
    }
  }

  private def growVariableLengthZone(newlyNeedeSize: Int): Unit ={
    if(currentUsedSizeInVariableLengthZone + newlyNeedeSize > variableLengthZoneSize) {
      val (newAddress, newSize) =
        Utils.grow(variableLengthZoneAddress, currentUsedSizeInVariableLengthZone, newlyNeedeSize)
      this.variableLengthZoneAddress = newAddress
      this.variableLengthZoneSize = newSize
    }
  }

  private[block] def setString(i:Int, j:Int, str: String):Unit = {
    val dataByteArray = str.map(_.toByte).toArray
    val stringSize = dataByteArray.length
    val stringOffset = currentUsedSizeInVariableLengthZone
    growVariableLengthZone(stringSize)
    Utils.copyMemory(dataByteArray, Utils.BYTE_ARRAY_OFFSET,
      null, variableLengthZoneAddress + stringOffset, stringSize.toLong)
    val offsetAndSize = stringOffset.toLong << 32 | stringSize.toLong
    Utils._UNSAFE.putLong(null, columnAddresses(j) + i * 8, offsetAndSize)
    currentUsedSizeInVariableLengthZone += stringSize
  }

  private[block] def set(i:Int, j:Int, value:Any, dataType:DataType):Unit = {
    dataType match{
      case BooleanType =>
        Utils._UNSAFE.putBoolean(null, columnAddresses(j) + i * Utils.sizeMap(BooleanType), value.asInstanceOf[Boolean])
      case IntegerType =>
        Utils._UNSAFE.putInt(null, columnAddresses(j) + i * Utils.sizeMap(IntegerType), value.asInstanceOf[Int])
      case LongType =>
        Utils._UNSAFE.putLong(null, columnAddresses(j) + i * Utils.sizeMap(LongType), value.asInstanceOf[Long])
      case FloatType =>
        Utils._UNSAFE.putFloat(null, columnAddresses(j) + i * Utils.sizeMap(FloatType), value.asInstanceOf[Float])
      case DoubleType =>
        Utils._UNSAFE.putDouble(null, columnAddresses(j) + i * Utils.sizeMap(DoubleType), value.asInstanceOf[Double])
      case StringType => setString(i, j, value.asInstanceOf[String])
      case _ => throw new NotImplementedError(s"${dataType} is not supported")
    }
  }

  override def add(row: InternalRow): Unit = {
    this.growColumns(1)
    val bitMaps = new Array[Long](bitMapSizeInWords)
    for(k <- bitMaps.indices) bitMaps(k) = 0L
    for(j <- schema.indices) {
      if (row.isNullAt(j)){
        val mask = 1L << (j & 0x3f)   // lgh: 1L << (j Mod 64)
        val wordOffset = j >> 6   // lgh: j / 64
        bitMaps(wordOffset) |= mask
        this.set(rowNum, j, 0L, LongType)
      }else {
        val dataType = schema(j).dataType
        val value = row.get(j, dataType)
        this.set(rowNum, j, value, dataType)
      }
    }
    Utils.copyMemory(bitMaps, Utils._UNSAFE.arrayBaseOffset(classOf[Array[Long]]),
      null, this.bitMapAddress + rowNum * bitMapSizeInBytes, bitMapSizeInBytes)
    rowNum += 1
  }

  override def build(): ColumnarInternalBlock = {

    val structType = this.schema
    val numFields = structType.length
    val columnAddresses = new Array[Long](numFields)
    for(j <- schema.indices) {
      val elemSize = Utils.sizeMap(schema(j).dataType)
      columnAddresses(j) = Utils._UNSAFE.allocateMemory(
        Utils.roundNumberOfBytesToNearestWord((rowNum * elemSize).toInt))  //TODO
      Utils.copyMemory(null, this.columnAddresses(j), null, columnAddresses(j), this.rowNum * elemSize)
    }
    val bitMapSizeInWords = Utils.calculateBitMapWidthInWords(numFields)
    val bitMapSizeInBytes = 8 * bitMapSizeInWords
    val bitMapAddress = Utils._UNSAFE.allocateMemory(rowNum * bitMapSizeInBytes)
    Utils.copyMemory(null, this.bitMapAddress, null, bitMapAddress, this.rowNum * bitMapSizeInBytes)
    val variableLengthZoneAddress =
      Utils._UNSAFE.allocateMemory(Utils.roundNumberOfBytesToNearestWord(currentUsedSizeInVariableLengthZone))
    Utils.copyMemory(null, this.variableLengthZoneAddress,
      null, variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)
    val block = new ColumnarInternalBlock(structType, bitMapAddress, columnAddresses, variableLengthZoneAddress)
    block.rowNum = this.rowNum
    block.bitMapSizeInBytes = bitMapSizeInBytes
    block.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone

    //    block.bitMapSizeInBytes = bitMapSizeInBytes
    //    block.bitMapAddress = Utils._UNSAFE.allocateMemory(block.rowNum * block.bitMapSizeInBytes)
    //    Utils.copyMemory(null, this.bitMapAddress, null, block.bitMapAddress, this.rowNum * bitMapSizeInBytes)

    //    block.variableLengthZoneAddress = Utils._UNSAFE.allocateMemory(
    //      Utils.roundNumberOfBytesToNearestWord(
    //        Utils.roundNumberOfBytesToNearestWord(this.currentUsedSizeInVariableLengthZone)))
    //    Utils.copyMemory(null, this.variableLengthZoneAddress,
    //      null, block.variableLengthZoneAddress, this.currentUsedSizeInVariableLengthZone)
    //    block.currentUsedSizeInVariableLengthZone = this.currentUsedSizeInVariableLengthZone

    block.dictionaryOrder = this.dictionaryOrder

    return block
  }

  override def finalize(): Unit = {
    Utils._UNSAFE.freeMemory(bitMapAddress)
    Utils._UNSAFE.freeMemory(variableLengthZoneAddress)
    columnAddresses.foreach(item => Utils._UNSAFE.freeMemory(item))
    super.finalize()
  }
}


//Abandoned code

//1)
//    def compare[T](a: T, b: T)(implicit ev$1: T => Ordered[T]): Int = {
//      if (a < b) return 1
//      else if(a == b) return 0
//      else return -1
//    }
//
//    def smaller(i: Int, j: Int): Boolean = {
//      var compareResult: Int = 0
//      for (item <- DictionaryOrder) {
//        val columnId = structType.getFieldIndex(item).get
//        val address = columnAddresses(columnId)
//        structType(item).dataType match{
//          case BooleanType =>
//            val (a,b) = (Utils._UNSAFE.getBooleanVolatile(null, address + i * Utils.sizeMap(BooleanType)),
//              Utils._UNSAFE.getBooleanVolatile(null, address + j * Utils.sizeMap(BooleanType)))
//            compareResult = Utils.compare(a,b)
//          case IntegerType =>
//            val (a ,b) = (Utils._UNSAFE.getInt(null, address + i * Utils.sizeMap(IntegerType)),
//              Utils._UNSAFE.getInt(null, address + j * Utils.sizeMap(IntegerType)))
//            compareResult = Utils.compare(a,b)
//          case LongType =>
//            val (a, b) = (Utils._UNSAFE.getLong(null, address + i * Utils.sizeMap(LongType)),
//              Utils._UNSAFE.getLong(null, address + j * Utils.sizeMap(LongType)))
//            compareResult = Utils.compare(a,b)
//          case FloatType =>
//            val (a, b) = (Utils._UNSAFE.getFloat(null, address + i * Utils.sizeMap(FloatType)),
//              Utils._UNSAFE.getFloat(null, address + j * Utils.sizeMap(FloatType)))
//            compareResult = Utils.compare(a, b)
//          case DoubleType =>
//            val (a, b) = (Utils._UNSAFE.getDouble(null, address + i * Utils.sizeMap(DoubleType)),
//              Utils._UNSAFE.getDouble(null, address + j * Utils.sizeMap(DoubleType)))
//            compareResult = Utils.compare(a, b)
//          case StringType => val (a, b) = (getString(i, columnId), getString(j, columnId))
//            compareResult = Utils.compare(a, b)
//          case _ => throw new NotImplementedError("Unhandled DataType encountered")
//          }
//        if(compareResult == 1) return true
//        else if(compareResult == -1) return false
//        }
//      return false
//    }

//    def swap_cascade(i: Int, j: Int): Unit = {
//      for(idx <- structType.indices){
//        val elemSize = Utils.sizeMap(structType(idx).dataType)
//        val temp = Utils._UNSAFE.allocateMemory(elemSize)
//        val (address_i, address_j) = (columnAddresses(idx) + i * elemSize, columnAddresses(idx) + j * elemSize)
//        Utils.swapMemory(address_i, address_j, temp, elemSize)
//        Utils._UNSAFE.freeMemory(temp)
//      }
//    }
//
//    def swap_baseline(i: Int, j: Int): Unit = {
//      val temp = Utils._UNSAFE.allocateMemory(4)
//      val (address_i, address_j) = (baselineAddress + i * 4, baselineAddress + j * 4)
//      Utils.swapMemory(address_i, address_j, temp, 4)
//      Utils._UNSAFE.freeMemory(temp)
//    }
