package org.apache.spark.secco.execution.storage

import org.apache.spark.secco.debug
import org.apache.spark.secco.execution.storage.row.InternalRow
import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.types._
import sun.misc.Unsafe

import java.util.Comparator

object Utils {

  var _UNSAFE: Unsafe = getUnsafe
  val BYTE_ARRAY_OFFSET: Long = _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])
  private val UNSAFE_COPY_THRESHOLD: Long = 1024L * 1024

  var sizeMap: Map[DataType, Long] = Map(
    IntegerType -> 4,
    LongType -> 8,
    BooleanType -> 1,
    FloatType -> 4,
    DoubleType -> 8,
    StringType -> 8
  )

  def calculateBitMapWidthInWords(numFields: Int): Int = (numFields + 63) / 64

  // lgh: this method is static in spark's Java implementation
  def calculateBitMapWidthInBytes(numFields: Int): Int =
    ((numFields + 63) / 64) * 8

  /** lgh: This method is from org.apache.spark.unsafe.array.ByteArrayMethods,
    * and is originally a static method with long as returned data type, in Java
    * @param numBytes
    * @return
    */
  def roundNumberOfBytesToNearestWord(numBytes: Int): Int = {
    val remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }
  def getUnsafe: Unsafe = {
    var f = classOf[Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    f.get(null).asInstanceOf[Unsafe]
  }

  def getStringAtColumnAddress(
      columnAddress: Long,
      variableLengthZoneAddress: Long,
      i: Int,
      elemSize: Int
  ): String = {
    val offsetAndSize =
      Utils._UNSAFE.getLong(null, columnAddress + i * elemSize)
    val stringAddress =
      variableLengthZoneAddress + (offsetAndSize >>> 32) //lgh: !!! the bracelet is necessary
    val stringSize = offsetAndSize.toInt
    val buf = new Array[Byte](stringSize)
    Utils.copyMemory(
      null,
      stringAddress,
      buf,
      Utils.BYTE_ARRAY_OFFSET,
      stringSize.toLong
    )
    val str = new String(buf)
    return str
  }

//
//  def getStringAtColumnAddress(
//      columnAddress: Long,
//      variableLengthZoneAddress: Long,
//      i: Int,
//      elemSize: Int
//  ): String = {
//    val offsetAndSize = Utils._UNSAFE.getLong(columnAddress + i * elemSize)
//    val stringAddress = variableLengthZoneAddress + (offsetAndSize >>> 32)   //lgh: !!! the bracelet is necessary
//    val stringSize = offsetAndSize.toInt
//    val buf = new Array[Byte](stringSize)
//    Utils.copyMemory(
//      null,
//      stringAddress,
//      buf,
//      Utils.BYTE_ARRAY_OFFSET,
//      stringSize.toLong
//    )
//    val str = new String(buf)
//    return str
//  }

  def calculateBitMapForRow(
      blockSchema: StructType,
      otherInternalRoW: InternalRow
  ): Array[Long] = {

    val fields = blockSchema.fields
    val bitMapData = new Array[Long](calculateBitMapWidthInWords(fields.length))
    var data = 0L
    for (ordinal <- fields.indices) {
      if (otherInternalRoW.isNullAt(ordinal)) data += (1 << (ordinal % 64))
      bitMapData(ordinal / 64) = data
      if ((ordinal + 1) % 64 == 0) {
        data = 0L
      }
    }
    bitMapData
  }

  // lgh: always use this method to allocate new memory
  // In this method, the newly allocated memory is immediately initialized with 0
  // In practice, some errors may occur if we don't do this initialization.
  def allocateMemory(size: Long): Long = {
    val address = _UNSAFE.allocateMemory(size)
    for (i <- 0.toLong until size) _UNSAFE.putByte(address + i, 0)
    address
  }

  /** lgh: This method is from org.apache.spark.unsafe.Platform, originally a static method of Platform.
    * lgh: This method capsulates Unsafe.copyMemory(Object, long, Object, long, long)
    * lgh: The comment below is from org.apache.spark.unsafe.Platform
    * Limits the number of bytes to copy per {@link Unsafe# copyMemory ( long, long, long)} to
    * allow safepoint polling during a large copy.
    */
  def copyMemory(
      src: AnyRef,
      srcOffset: Long,
      dst: AnyRef,
      dstOffset: Long,
      length: Long
  ): Unit = {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
//    println("copying memory happened")
    var lengthRemained = length
    var curSrcOffset = srcOffset
    var curDstOffset = dstOffset

    if (curDstOffset < curSrcOffset)
      while (lengthRemained > 0) {
        val size = Math.min(lengthRemained, UNSAFE_COPY_THRESHOLD)
        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
        lengthRemained -= size
        curSrcOffset += size
        curDstOffset += size
      }
    else {
      curSrcOffset += lengthRemained
      curDstOffset += lengthRemained
      while (lengthRemained > 0) {
        val size = Math.min(lengthRemained, UNSAFE_COPY_THRESHOLD)
        curSrcOffset -= size
        curDstOffset -= size
        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
        lengthRemained -= size
      }
    }
  }

  def swapMemory(a: Long, b: Long, temp: Long, size: Long): Unit = {
    copyMemory(null, a, null, temp, size)
    copyMemory(null, b, null, a, size)
    copyMemory(null, temp, null, b, size)
  }

  def grow(address: Long, usedSize: Int, newlyNeededSize: Int): (Long, Int) = {
    val ARRAY_MAX = Integer.MAX_VALUE - 15
    if (newlyNeededSize < 0) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size is negative"
      )
    }
    if (newlyNeededSize > ARRAY_MAX - usedSize) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX
      )
    }
    println("growing memory happened")
    val length: Int = usedSize + newlyNeededSize
    // This will not happen frequently, because the buffer is re-used.
    var newLength: Int = 0
    if (length < ARRAY_MAX / 2) newLength = length * 2
    else newLength = ARRAY_MAX
    val roundedSize = Utils.roundNumberOfBytesToNearestWord(newLength)

//    val newAddress = Utils._UNSAFE.allocateMemory(roundedSize.toLong)
    val newAddress = Utils.allocateMemory(roundedSize.toLong)
    Utils.copyMemory(null, address, null, newAddress, usedSize)
    Utils._UNSAFE.freeMemory(address)
    return (newAddress, roundedSize)
  }

  def anyCompare(a: Any, b: Any): Int = {
    (a, b) match {
      case (aInt: Int, bInt: Int) =>
        if (aInt < bInt) return -1 else if (aInt > bInt) return 1
      case (aBoolean: Boolean, bBoolean: Boolean) =>
        if (aBoolean < bBoolean) return -1
        else if (aBoolean > bBoolean) return 1
      case (aDouble: Double, bDouble: Double) =>
        if (aDouble < bDouble) return -1 else if (aDouble > bDouble) return 1
      case (aFloat: Float, bFloat: Float) =>
        if (aFloat < bFloat) return -1 else if (aFloat > bFloat) return 1
      case (aLong: Long, bLong: Long) =>
        if (aLong < bLong) return -1 else if (aLong > bLong) return 1
      case (aString: String, bString: String) =>
        if (aString < bString) return -1 else if (aString > bString) return 1
      case _ =>
        throw new NotImplementedError(
          s"Data types not supported (${a.getClass} and ${b.getClass}"
        )
    }
    return 0
  }

  def compare[T](a: T, b: T)(implicit ev$1: T => Ordered[T]): Int = {
    if (a < b) return 1
    else if (a == b) return 0
    else return -1
  }

  def anyToLongForComparison(in: Any): Long = {
    val tempAddress: Long = _UNSAFE.allocateMemory(8)

    in match {
      case inLong: Long     => _UNSAFE.putLong(tempAddress, inLong)
      case inDouble: Double => _UNSAFE.putDouble(tempAddress, inDouble)
      case inInt: Int =>
        _UNSAFE.putInt(tempAddress, 0)
        _UNSAFE.putInt(tempAddress + 4, inInt)
      case inFloat: Float =>
        _UNSAFE.putFloat(tempAddress, 0f)
        _UNSAFE.putFloat(tempAddress + 4, inFloat)
      case inBoolean: Boolean =>
        _UNSAFE.putLong(tempAddress, 0L)
        _UNSAFE.putBoolean(null, tempAddress, inBoolean)
      case _ => throw new NotImplementedError()
    }

    val out: Long = _UNSAFE.getLong(tempAddress)
    _UNSAFE.freeMemory(tempAddress)
    out
  }

  class InternalRowComparator(rowSchema: StructType)
      extends Comparator[InternalRow]
      with Serializable {

    var indexMapArray: Array[Int] = {
      val array = new Array[Int](rowSchema.length);
      for (idx <- 0 until rowSchema.length) array(idx) = idx;
      array
    }

    var directions: Array[Boolean] = rowSchema.fields.map(_ => true)

    // later added by lgh
    def this(rowSchema: StructType, indexMap: Array[Int]) {
      this(rowSchema)
      indexMapArray = indexMap
    }

    // later added by lgh
    def this(rowSchema: StructType, sortDirections: Array[Boolean]) {
      this(rowSchema)
      directions = sortDirections
    }

    override def compare(
        o1: InternalRow,
        o2: InternalRow
    ): Int = {

      var i = 0
      while (i < rowSchema.length) {
        val dataType = rowSchema(indexMapArray(i)).dataType
        val item1: Any = o1.get(indexMapArray(i), dataType)
        val item2: Any = o2.get(indexMapArray(i), dataType)
        val compResult = Utils.anyCompare(item1, item2)
        if (compResult != 0) {
//          return compResult
          return if (directions(i)) compResult
          else -compResult // later added by lgh
        } else {
          i += 1
        }
      }
      return 0
    }
  }

//  def attributeArrayToStructType(attributeArray: Array[Attribute]): StructType = StructType(attributeArray.map{
//    i => StructField(i.name, i.dataType, i.nullable)
//  })
//  {
//    val structFieldsArray = {
//      //lgh DONE: consider when localAttributeOrder elements are not instances of AttributeReference
////      attributeArray.map(_.asInstanceOf[AttributeReference]).map(i => StructField(i.name, i.dataType))
//      attributeArray.map(i => StructField(i.name, i.dataType, i.nullable))
//    }
//    StructType(structFieldsArray)
//  }
}
