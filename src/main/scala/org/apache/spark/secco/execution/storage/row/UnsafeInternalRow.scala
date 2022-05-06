package org.apache.spark.secco.execution.storage.row

import org.apache.spark.secco.types.{DataType, _}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import sun.misc.Unsafe
import org.apache.spark.secco.execution.storage.Utils

import scala.collection.immutable.HashSet
import scala.collection.mutable

class UnsafeInternalRow extends InternalRow {

  /** lgh: This constructor is similar to that of org.apache.spark.sql.catalyst.expressions.UnsafeRow
    *
    * Construct a new UnsafeRow. The resulting row won't be usable until `initWith()` has been called,
    * since the value returned by this constructor is equivalent to a null pointer.
    *
    * @param numFields the number of fields in this row
    */
  private val _UNSAFE: Unsafe = UnsafeInternalRow._UNSAFE
//  private val BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])
//  private val UNSAFE_COPY_THRESHOLD = 1024L * 1024

  private[storage] var baseObject: AnyRef = _
  private[storage] var baseOffset: Long = _

  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
  private var numberOfFields: Int = _

  /** The size of this row's backing data, in bytes) */
  private var sizeInBytes: Int = _

  /** The width of the null tracking bit set, in bytes */
  private[storage] var bitSetWidthInBytes: Int = _

  private[storage] var currentUsedSize: Int = _

  private var fragmentSeq: mutable.Seq[Long] = mutable.Seq()

  def this(numFields: Int) = {
    this()
    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
    val bitMapWidthInWords = Utils.calculateBitMapWidthInWords(numFields)
    this.numberOfFields = numFields
    this.bitSetWidthInBytes = bitMapWidthInWords * 8
  }

  def this(numFields: Int, autoInit: Boolean) = {
    this(numFields)
    if (autoInit) {
      this.sizeInBytes = UnsafeInternalRow.roundNumberOfBytesToNearestWord(
        bitSetWidthInBytes + (numFields * 8) * 3
      )
      this.currentUsedSize = bitSetWidthInBytes + numberOfFields * 8

      this.baseObject = null
      this.baseOffset = Utils.allocateMemory(sizeInBytes.toLong)

      // 29/11/2021 lgh: set notNull at all fields, to make it support GenerateUnsafeInternalRowJoiner
      for (i <- 0 until Utils.calculateBitMapWidthInWords(numFields))
        _UNSAFE.putLong(baseObject, baseOffset + i * 8, 0L)
    }
  }

  def this(numFields: Int, size: Int) = {
    this(numFields)
    this.sizeInBytes = UnsafeInternalRow.roundNumberOfBytesToNearestWord(size)
    this.currentUsedSize = bitSetWidthInBytes + numberOfFields * 8

    assert(
      sizeInBytes >= currentUsedSize,
      "current size (rounded) is not enough even for fixed-length data"
    )

    this.baseObject = null
    this.baseOffset = Utils.allocateMemory(sizeInBytes.toLong)

    // 29/11/2021 lgh: set notNull at all fields, to make it support GenerateUnsafeInternalRowJoiner
    for (i <- 0 until Utils.calculateBitMapWidthInWords(numFields))
      _UNSAFE.putLong(baseObject, baseOffset + i * 8, 0L)
  }

  /** lgh: this method is from spark's UnsafeRow.pointTo(AnyRef, Long, Int), added at 29/11/2021, below is the original comment:
    * Update this UnsafeRow to point to different backing data.
    *
    * @param baseObject  the base object
    * @param baseOffset  the offset within the base object
    * @param sizeInBytes the size of this row's backing data, in bytes
    */
  def pointTo(baseObject: AnyRef, baseOffset: Long, sizeInBytes: Int): Unit = {
    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
    assert(
      sizeInBytes % 8 == 0,
      "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8"
    )
    this.baseObject = baseObject
    this.baseOffset = baseOffset
    this.sizeInBytes = sizeInBytes
    this.currentUsedSize = sizeInBytes
  }

  /** Update this UnsafeRow to point to the underlying byte array.
    *
    * @param buf byte array to point to
    * @param sizeInBytes the number of bytes valid in the byte array
    */
  def pointTo(buf: Array[Byte], sizeInBytes: Int): Unit = {
    pointTo(buf, UnsafeInternalRow.BYTE_ARRAY_OFFSET, sizeInBytes);
  }

  def setTotalSize(sizeInBytes: Int): Unit = {
    assert(
      sizeInBytes % 8 == 0,
      "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8"
    )
    this.sizeInBytes = sizeInBytes
  }

  /** The following 4 getters are added by lgh at 29/11/2021, to support GenerateUnsafeInternalRowJoiner.
    */

  def getSizeInBytes: Int = sizeInBytes

  def getBaseObject: AnyRef = baseObject

  def getBaseOffset: Long = baseOffset

  /** lgh: this method is from spark's UnsafeRow.pointTo(Array[Byte], Int), added at 29/11/2021, below is the original comment:
    * Update this UnsafeRow to point to the underlying byte array.
    *
    * @param buf         byte array to point to
    * @param sizeInBytes the number of bytes valid in the byte array
    */
  def initWithByteArray(buf: Array[Byte], sizeInBytes: Int): Unit = {
    pointTo(buf, UnsafeInternalRow.BYTE_ARRAY_OFFSET, sizeInBytes)
  }

  //////////////////////////////////////////////////////////////////////////////
  // Private fields and methods
  //////////////////////////////////////////////////////////////////////////////

  private[storage] def getFieldOffset(ordinal: Int): Long =
    baseOffset + bitSetWidthInBytes + ordinal * 8L

  private def assertIndexIsValid(index: Int): Unit = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    assert(
      index < numFields,
      "index (" + index + ") should < " + numberOfFields
    )
  }

  def currentCursor: Int = currentUsedSize

  /** Sets the bit at the specified index to {@code true}.
    */
  private def setBitMap(index: Int): Unit = {
    //    assertIndexIsValid(index)
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * 8
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    _UNSAFE.putLong(baseObject, wordOffset, word | mask)
  }

  /** Sets the bit at the specified index to {@code false}.
    */
  private def unsetBitMap(index: Int): Unit = {
    //    assertIndexIsValid(index)
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * 8
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    _UNSAFE.putLong(baseObject, wordOffset, word & ~mask)
  }

  private def bitMapIsSetAt(index: Int): Boolean = {
    //    assertIndexIsValid(index)
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * 8
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    return (mask & word) != 0L
  }

  /** Sets the bit at the specified index to {@code true} if it was fasle.
    */
  private def setBitMapIfNotSet(index: Int): Unit = {
    //    assertIndexIsValid(index)
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * 8
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    if ((mask & word) == 0L)
      _UNSAFE.putLong(baseObject, wordOffset, word | mask)
  }

  /** Sets the bit at the specified index to {@code false} if it was true.
    */
  private def unsetBitMapIfSet(index: Int): Unit = {
    assertIndexIsValid(index)
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * 8
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    if ((mask & word) != 0L)
      _UNSAFE.putLong(baseObject, wordOffset, word & ~mask)
  }

  /** lgh: This method is pted from org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder.grow(int)
    * lgh: In this method, we first calculate the total length we need (size already used + newlyNeededSize). Then, we
    * compare it with the already allocated size (some allocated memory may have not been used so dose not account for
    * "currentUsedSize"). If the total size we need now exceeds the size already allocated, we will create a new backing
    * Array[Byte] with enough memory size allocated and re-initialize this UnsafeInternalRow with the new Array[Byte].
    *
    * When ere is "really" a need to grow the size, the newly obtained total size will be 2 * total_size_we_neeed_now,
    * rounded to the nearest word size ( a multiple of 8, with 8 bytes as a word ). The maximum size we can allocate to
    * a row is ARRAY_MAX = Integer.MAX_VALUE - 15 = 2147483632 (bytes)
    * @param newlyNeededSize
    */
  private[storage] def grow(newlyNeededSize: Int): Unit = {
    val ARRAY_MAX = Integer.MAX_VALUE - 15
    //    val ARRAY_MAX = Integer.MAX_VALUE /  2 - 63
    if (newlyNeededSize < 0) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size is negative"
      )
    }
    if (newlyNeededSize > ARRAY_MAX - currentUsedSize) {
      throw new IllegalArgumentException(
        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size after growing " +
          "exceeds size limitation " + ARRAY_MAX
      )
    }
    val length: Int = currentUsedSize + newlyNeededSize
    if (sizeInBytes < length) {
      // This will not happen frequently, because the buffer is re-used.
      var newLength: Int = 0
      if (length < ARRAY_MAX / 2) newLength = length * 2
      else newLength = ARRAY_MAX
      val roundedSize =
        UnsafeInternalRow.roundNumberOfBytesToNearestWord(newLength)

      val newBaseObject = null
      val newAddress = Utils.allocateMemory(roundedSize.toLong)
      Utils.copyMemory(
        baseObject,
        baseOffset,
        newBaseObject,
        newAddress,
        currentUsedSize
      )
      if (baseObject == null) {
        _UNSAFE.freeMemory(baseOffset)
      }
      baseObject = newBaseObject
      baseOffset = newAddress
    }
  }

  /** lgh: This method is from org.apache.spark.unsafe.Platform, originally a static method of Platform.
    * lgh: This method capsulates Unsafe.copyMemory(Object, long, Object, long, long)
    * lgh: The comment below is from org.apache.spark.unsafe.Platform
    * Limits the number of bytes to copy per {@link Unsafe# copyMemory ( long, long, long)} to
    * allow safepoint polling during a large copy.
    */
  private def copyMemory(
      src: AnyRef,
      srcOffset: Long,
      dst: AnyRef,
      dstOffset: Long,
      length: Long
  ): Unit = {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    var lengthRemained = length
    var curSrcOffset = srcOffset
    var curDstOffset = dstOffset

    if (curDstOffset < curSrcOffset)
      while (lengthRemained > 0) {
        val size =
          Math.min(lengthRemained, UnsafeInternalRow.UNSAFE_COPY_THRESHOLD)
        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
        lengthRemained -= size
        curSrcOffset += size
        curDstOffset += size
      }
    else {
      curSrcOffset += lengthRemained
      curDstOffset += lengthRemained
      while (lengthRemained > 0) {
        val size =
          Math.min(lengthRemained, UnsafeInternalRow.UNSAFE_COPY_THRESHOLD)
        curSrcOffset -= size
        curDstOffset -= size
        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
        lengthRemained -= size
      }
    }
  }

  override def numFields: Int = numberOfFields

  override def setNullAt(i: Int): Unit = {
    //    assertIndexIsValid(i)
    setBitMap(i)
    _UNSAFE.putLong(baseObject, getFieldOffset(i), 0)
  }

  def setNotNullAt(i: Int): Unit = {
    //    assertIndexIsValid(i)
    unsetBitMap(i)
  }

  def setNotNullIfNullAt(i: Int): Unit = {
    //    assertIndexIsValid(i)
    unsetBitMapIfSet(i)
  }

  /** Updates the value at column `i`. Note that after updating, the given value will be kept in this
    * row, and the caller side should guarantee that this value won't be changed afterwards.
    */
  override def update(i: Int, value: Any): Unit = {

    if (!value.isInstanceOf[String]) {
      setNotNullIfNullAt(i)
      value match {
        case vBool: Boolean =>
          _UNSAFE.putBoolean(baseObject, getFieldOffset(i), vBool)
        case vByte: Byte =>
          _UNSAFE.putByte(baseObject, getFieldOffset(i), vByte)
        case vShort: Short =>
          _UNSAFE.putShort(baseObject, getFieldOffset(i), vShort)
        case vInt: Int => _UNSAFE.putInt(baseObject, getFieldOffset(i), vInt)
        case vLong: Long =>
          _UNSAFE.putLong(baseObject, getFieldOffset(i), vLong)
        case vFloat: Float =>
          _UNSAFE.putFloat(baseObject, getFieldOffset(i), vFloat)
        case vDouble: Double =>
          _UNSAFE.putDouble(baseObject, getFieldOffset(i), vDouble)
        case _ =>
      }
    } else setString(i, value.asInstanceOf[String])
  }

  override def setBoolean(i: Int, value: Boolean): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putBoolean(baseObject, getFieldOffset(i), value)
  }

  override def setByte(i: Int, value: Byte): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putByte(baseObject, getFieldOffset(i), value)
  }

  override def setInt(i: Int, value: Int): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putInt(baseObject, getFieldOffset(i), value)
  }

  override def setLong(i: Int, value: Long): Unit =
    _UNSAFE.putLongVolatile(baseObject, getFieldOffset(i), value)

  override def setDouble(i: Int, value: Double): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putDouble(baseObject, getFieldOffset(i), value)
  }

  override def setFloat(i: Int, value: Float): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putFloat(baseObject, getFieldOffset(i), value)
  }

  override def setShort(i: Int, value: Short): Unit = {
    setNotNullIfNullAt(i)
    _UNSAFE.putShort(baseObject, getFieldOffset(i), value)
  }

  private[storage] def writeString(
      i: Int,
      byteArray: Array[Byte],
      offset: Long,
      size: Long
  ): Unit = {
    val offsetAndSize = offset << 32 | size
    _UNSAFE.putLong(baseObject, getFieldOffset(i), offsetAndSize)
    Utils.copyMemory(
      byteArray,
      UnsafeInternalRow.BYTE_ARRAY_OFFSET,
      baseObject,
      baseOffset + offset,
      size
    )
  }

  override def setString(i: Int, vStr: String): Unit = {

    //    assertIndexIsValid(i)
    val dataByteArray = vStr.map(_.toByte).toArray
    val stringSize = dataByteArray.length

    //lgh: If the field is not null (we assume that it must be a string that has been set here), we try to put new
    // string at the original place. If the original place does not have enough space, we will collect it for
    // future use.
    if (!isNullAt(i)) {

      val originalOffsetAndSize: Long = getLong(i)
      val originalSize: Int = originalOffsetAndSize.toInt
      val originalOffset: Long = originalOffsetAndSize >>> 32

      if (originalSize >= stringSize) {

        setNotNullIfNullAt(i)
        writeString(i, dataByteArray, originalOffset, stringSize.toLong)

        if (originalSize - stringSize > 7) { // lgh: to avoid too many tiny fragments
          val fragOffsetAndSize =
            (originalOffset + stringSize) << 32 | originalSize - stringSize
          fragmentSeq = (fragmentSeq :+ fragOffsetAndSize).sortBy(_ >>> 32)
        }

        return

      } else { //originalSize < stringSize, collect it to fragmentSeq for future use
        fragmentSeq = (fragmentSeq :+ originalOffsetAndSize).sortBy(_ >>> 32)
      }

    }

    // lgh: If the program runs to here, it means that we can not put the string at "original" place. We attempt to
    // find a fragment that can contain the string.
    val max_idx = fragmentSeq.length - 1
    for (idx <- 0 to max_idx) {
      val fragmentOffset: Long = fragmentSeq(idx) >>> 32
      val fragmentSize = fragmentSeq(idx).toInt
      if (fragmentSize >= stringSize) {

        setNotNullIfNullAt(i)
        writeString(i, dataByteArray, fragmentOffset, stringSize.toLong)

        val updatedFragOffset: Long = (fragmentOffset + stringSize) << 32
        val updatedFragOffsetAndSize: Long =
          updatedFragOffset | fragmentSize.toLong
        fragmentSeq(idx) = updatedFragOffsetAndSize
        fragmentSeq.dropWhile(_.toInt == 0)

        return

      }
    }

    // lgh: If the method has not returned, it means that we can not find an adequate
    // place in the region that has been used. We try expanding our "boundary".
    this.grow(stringSize)
    setNotNullIfNullAt(i)
    writeString(i, dataByteArray, currentCursor.toLong, stringSize.toLong)
    this.currentUsedSize += stringSize
  }

  /** Make a copy of the current [[InternalRow]] object.
    */
  override def copy(): InternalRow = {
    val rowCopy = new UnsafeInternalRow(numberOfFields, this.sizeInBytes)
    rowCopy.currentUsedSize = this.currentUsedSize
    Utils.copyMemory(
      baseObject,
      baseOffset,
      rowCopy.baseObject,
      rowCopy.baseOffset,
      currentUsedSize
    )
    return rowCopy;
  }

  override def isNullAt(ordinal: Int): Boolean = bitMapIsSetAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean =
    _UNSAFE.getBoolean(baseObject, getFieldOffset(ordinal))

  override def getInt(ordinal: Int): Int =
    _UNSAFE.getInt(baseObject, getFieldOffset(ordinal))

  override def getLong(ordinal: Int): Long =
    _UNSAFE.getLong(baseObject, getFieldOffset(ordinal))

  override def getFloat(ordinal: Int): Float =
    _UNSAFE.getFloat(baseObject, getFieldOffset(ordinal))

  override def getDouble(ordinal: Int): Double =
    _UNSAFE.getDouble(baseObject, getFieldOffset(ordinal))

  override def getString(ordinal: Int): String = {
    //    assertIndexIsValid(ordinal)
    val isNUll = isNullAt(ordinal)
    if (isNUll) return null
    val offsetAndSize: Long = getLong(ordinal)
    val offset: Long = (offsetAndSize >> 32).toInt
    val size: Int = offsetAndSize.toInt
    val buf: Array[Byte] = new Array[Byte](size)
    Utils.copyMemory(
      baseObject,
      baseOffset + offset,
      buf,
      UnsafeInternalRow.BYTE_ARRAY_OFFSET,
      size
    )
    return new String(buf)
  }

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    //    assertIndexIsValid(ordinal)
    dataType match {
      case BooleanType => getBoolean(ordinal).asInstanceOf[AnyRef]
      case IntegerType => getInt(ordinal).asInstanceOf[AnyRef]
      case LongType    => getLong(ordinal).asInstanceOf[AnyRef]
      case FloatType   => getFloat(ordinal).asInstanceOf[AnyRef]
      case DoubleType  => getDouble(ordinal).asInstanceOf[AnyRef]
      case StringType  => getString(ordinal).asInstanceOf[AnyRef]
      case _           => getLong(ordinal).asInstanceOf[AnyRef]
      //    case AnyDataType =>
      //    case NumericType =>
    }
  }

  override def toString: String = {
    val build = new StringBuilder("[")
    var i = 0
    while (i < sizeInBytes) {
      if (i != 0) build.append(',')
      build.append(
        java.lang.Long.toHexString(_UNSAFE.getLong(baseObject, baseOffset + i))
      )

      i += 8
    }
    build.append(']')
    build.toString
  }

  private[storage] def show(schema: StructType): Unit = {
    val build = new StringBuilder("[")
    val fields = schema.fields
    for (ordinal <- fields.indices) {
      if (ordinal != 0) build.append(",")
      val dataType = fields(ordinal).dataType
      val value: Any = if (isNullAt(ordinal)) {
        "null"
      } else {
        dataType match {
          case BooleanType => getBoolean(ordinal)
          case IntegerType => getInt(ordinal)
          case LongType    => getLong(ordinal)
          case FloatType   => getFloat(ordinal)
          case DoubleType  => getDouble(ordinal)
          case StringType  => getString(ordinal)
          case _           => "unSupportType"
        }
      }
      build.append(value.toString)
    }
    build.append("]")
    println(build.toString)
  }

  override def hashCode: Int =
    Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42)

  override def equals(other: Any): Boolean = {
    other match {
      case o: UnsafeInternalRow =>
        return (sizeInBytes == o.sizeInBytes) &&
          ByteArrayMethods.arrayEquals(
            baseObject,
            baseOffset,
            o.baseObject,
            o.baseOffset,
            sizeInBytes
          )
      case _ =>
    }
    false
  }

  private[storage] def copyBitMapFrom(
      baseRef: AnyRef = null,
      address: Long
  ): Unit =
//    Utils._UNSAFE.copyMemory(baseRef, address, baseObject, baseOffset, bitSetWidthInBytes)
    Utils.copyMemory(
      baseRef,
      address,
      baseObject,
      baseOffset,
      bitSetWidthInBytes
    )

  private[storage] def copyLongOrDoubleFrom(
      baseRef: AnyRef = null,
      address: Long,
      i: Int
  ): Unit =
//    Utils._UNSAFE.copyMemory(baseRef, address, baseObject, getFieldOffset(i), 8)
    Utils.copyMemory(baseRef, address, baseObject, getFieldOffset(i), 8)

  private[storage] def copyIntOrFloatFrom(
      baseRef: AnyRef = null,
      address: Long,
      i: Int
  ): Unit =
//    Utils._UNSAFE.copyMemory(baseRef, address, baseObject, getFieldOffset(i) + 4, 4)
    Utils.copyMemory(baseRef, address, baseObject, getFieldOffset(i), 4)

  private[storage] def copyBooleanOrByteFrom(
      baseRef: AnyRef = null,
      address: Long,
      i: Int
  ): Unit =
//    Utils._UNSAFE.copyMemory(baseRef, address, baseObject, getFieldOffset(i) + 7, 1)
    Utils.copyMemory(baseRef, address, baseObject, getFieldOffset(i), 1)

  private[storage] def copyDataFromWithDataSize(
      baseRef: AnyRef = null,
      address: Long,
      elemSize: Int,
      i: Int
  ): Unit =
//    Utils._UNSAFE.copyMemory(baseRef, address, baseObject, getFieldOffset(i) + 8 - elemSize, elemSize)
    Utils.copyMemory(baseRef, address, baseObject, getFieldOffset(i), elemSize)

  private[storage] def copyStringFrom(
      variableLengthZoneAddress: Long,
      offsetAndSize: Long,
      i: Int
  ): Unit = {
    val stringAddress = variableLengthZoneAddress + (offsetAndSize >> 32)
    val stringSize = offsetAndSize.toInt
    val newOffsetAndSize = currentCursor.toLong << 32 | stringSize.toLong
    grow(stringSize)
    Utils._UNSAFE.putLong(baseObject, getFieldOffset(i), newOffsetAndSize)
    Utils.copyMemory(
      null,
      stringAddress,
      baseObject,
      baseOffset + currentCursor,
      stringSize
    )
    currentUsedSize += stringSize
  }

//  override def finalize(): Unit = {
//    println("UnsafeInternalRow: finalizing ...")
//    if(baseObject == null) {
//      println("freeMemory...")
//      _UNSAFE.freeMemory(baseOffset)
//    }
//    super.finalize()
//  }

}

object UnsafeInternalRow {
  val fixedLengthZoneTypes: Set[DataType] =
    HashSet[DataType](BooleanType, DoubleType, FloatType, IntegerType, LongType)

  private def getUnsafe: Unsafe = {
    val f = classOf[Unsafe].getDeclaredField("theUnsafe")
    f.setAccessible(true)
    return f.get(null).asInstanceOf[Unsafe]
  }

  val _UNSAFE: Unsafe = getUnsafe
  val UNSAFE: Unsafe = _UNSAFE
  val BYTE_ARRAY_OFFSET: Int = _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])
  val UNSAFE_COPY_THRESHOLD: Long = 1024L * 1024

  //lgh TODO: currently we only support 6 data types, and all of them except for StrintType are fix-lengthed.
  def isFixedLength(dt: DataType): Boolean = fixedLengthZoneTypes.contains(dt)

  def fromInternalRow(
      schema: StructType,
      row: InternalRow
  ): UnsafeInternalRow = {
    if (row.isInstanceOf[UnsafeInternalRow]) {
      return row.asInstanceOf[UnsafeInternalRow]
    }
    val numFields = schema.length
    val dataTypes = schema.map(_.dataType)
    var unsafeRow: UnsafeInternalRow = null
    if (dataTypes.contains(StringType))
      unsafeRow = new UnsafeInternalRow(numFields, autoInit = true)
    else
      unsafeRow = new UnsafeInternalRow(
        numFields,
        8 * numFields + Utils.calculateBitMapWidthInBytes(numFields)
      )
    for (i <- dataTypes.indices) {
      if (row.isNullAt(i)) {
        unsafeRow.setNullAt(i)
      } else {
        dataTypes(i) match {
          case BooleanType => unsafeRow.setBoolean(i, row.getBoolean(i))
          case IntegerType => unsafeRow.setInt(i, row.getInt(i))
          case LongType    => unsafeRow.setLong(i, row.getLong(i))
          case FloatType   => unsafeRow.setFloat(i, row.getFloat(i))
          case DoubleType  => unsafeRow.setDouble(i, row.getDouble(i))
          case StringType  => unsafeRow.setString(i, row.getString(i))
          case dt          => throw new NotImplementedError(s"Unsupported Type: $dt")
        }
      }
    }
    unsafeRow
  }

  // lgh: this method is static in spark's Java implementation
  def calculateBitMapWidthInBytes(numFields: Int): Int =
    ((numFields + 63) / 64) * 8

  def calculateBitMapWidthInWords(numFields: Int): Int = (numFields + 63) / 64

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
}

//package org.apache.spark.secco.execution.storage.row
//
//import org.apache.spark.secco.execution.storage.Utils
//import org.apache.spark.secco.types._
//import org.apache.spark.unsafe.array.ByteArrayMethods
//import org.apache.spark.unsafe.hash.Murmur3_x86_32
//import sun.misc.Unsafe
//
//import java.util.Collections
//import scala.collection.immutable.HashSet
//import scala.collection.mutable
//
//class UnsafeInternalRow extends InternalRow {
//
//  /** lgh: This constructor is similar to that of org.apache.spark.sql.catalyst.expressions.UnsafeRow
//    *
//    * Construct a new UnsafeRow. The resulting row won't be usable until `initWith()` has been called,
//    * since the value returned by this constructor is equivalent to a null pointer.
//    *
//    * @param numFields the number of fields in this row
//    */
//  private val _UNSAFE: Unsafe = getUnsafe
//  private val BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])
//  private val UNSAFE_COPY_THRESHOLD = 1024L * 1024
//
//  private[storage] var baseObject: AnyRef = _
//  private[storage] var baseOffset: Long = _
//
//  /** The number of fields in this row, used for calculating the bitset width (and in assertions) */
//  private var numberOfFields: Int = _
//
//  /** The size of this row's backing data, in bytes) */
//  private var sizeInBytes: Int = _
//
//  /** The width of the null tracking bit set, in bytes */
//  private[storage] var bitSetWidthInBytes: Int = _
//
//  private[storage] var currentUsedSize: Int = _
//
//  private var fragmentSeq: mutable.Seq[Long] = mutable.Seq()
//
//  def this(numFields: Int) = {
//    this()
//    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
//    val bitMapWidthInWords = calculateBitMapWidthInWords(numFields)
//    this.numberOfFields = numFields
//    this.bitSetWidthInBytes = bitMapWidthInWords * 8
//  }
//
//  def this(numFields: Int, autoInit: Boolean) = {
//    this(numFields)
////    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
////    val bitMapWidthInWords = calculateBitMapWidthInWords(numFields)
////    this.numberOfFields = numFields
////    this.bitSetWidthInBytes = bitMapWidthInWords * 8
//    if (autoInit) {
//      this.sizeInBytes = roundNumberOfBytesToNearestWord(
//        bitSetWidthInBytes + (numFields * 8) * 3
//      )
//      this.currentUsedSize = bitSetWidthInBytes + numberOfFields * 8
//
//      this.baseObject = null
//      this.baseOffset = Utils.allocateMemory(sizeInBytes.toLong)
//
//      //set null at all fields
//      for (i <- 0 until calculateBitMapWidthInWords(numFields))
//        _UNSAFE.putLong(baseObject, baseOffset + i * 8, 0xffffffffffffffffL)
//    }
//  }
//
//  def this(numFields: Int, size: Int) = {
//    this(numFields)
////    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
////    //    assert (sizeInBytes % 8 == 0, "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8")
////
////    val bitMapWidthInWords = calculateBitMapWidthInWords(numFields)
////
////    this.numberOfFields = numFields
////    this.bitSetWidthInBytes = bitMapWidthInWords * 8
//    this.sizeInBytes = roundNumberOfBytesToNearestWord(size)
//    this.currentUsedSize = bitSetWidthInBytes + numberOfFields * 8
//
//    assert(
//      sizeInBytes >= currentUsedSize,
//      "current size (rounded) is not enough even for fixed-length data"
//    )
//
//    this.baseObject = null
//    this.baseOffset = Utils.allocateMemory(sizeInBytes.toLong)
//
//    //set null at all fields
//    for (i <- 0 until calculateBitMapWidthInWords(numFields))
//      _UNSAFE.putLong(baseObject, baseOffset + i * 8, 0xffffffffffffffffL)
//  }
//
//  /**
//    * lgh: this method is from spark's UnsafeRow.pointTo(AnyRef, Long, Int), added at 29/11/2021, below is the original comment:
//    * Update this UnsafeRow to point to different backing data.
//    *
//    * @param baseObject  the base object
//    * @param baseOffset  the offset within the base object
//    * @param sizeInBytes the size of this row's backing data, in bytes
//    */
//  def pointTo(baseObject: AnyRef, baseOffset: Long, sizeInBytes: Int): Unit = {
//    assert(numFields >= 0, "numFields (" + numFields + ") should >= 0")
//    assert(sizeInBytes % 8 == 0, "sizeInBytes (" + sizeInBytes + ") should be a multiple of 8")
//    this.baseObject = baseObject
//    this.baseOffset = baseOffset
//    this.sizeInBytes = sizeInBytes
//  }
//
//  /**
//    * The following 4 getters are added by lgh at 29/11/2021, to support GenerateUnsafeInternalRowJoiner.
//    */
//
//  def getSizeInBytes: Int = sizeInBytes
//
//  def getBaseObject: AnyRef = baseObject
//
//  def getBaseOffset: Long = baseOffset
//
//  /**
//    * lgh: this method is from spark's UnsafeRow.pointTo(Array[Byte], Int), added at 29/11/2021, below is the original comment:
//    * Update this UnsafeRow to point to the underlying byte array.
//    *
//    * @param buf         byte array to point to
//    * @param sizeInBytes the number of bytes valid in the byte array
//    */
//  def initWithByteArray(buf: Array[Byte], sizeInBytes: Int): Unit = {
//    pointTo(buf, BYTE_ARRAY_OFFSET, sizeInBytes)
//  }
//
////  def initWithByteArray(array: Array[Byte], length: Int): Unit = {
////    baseObject = array
////    baseOffset = _UNSAFE.arrayBaseOffset(classOf[Array[Byte]])
////  }
//
//  //////////////////////////////////////////////////////////////////////////////
//  // Private fields and methods
//  //////////////////////////////////////////////////////////////////////////////
//
//  private def getUnsafe: Unsafe = {
//    val f = classOf[Unsafe].getDeclaredField("theUnsafe")
//    f.setAccessible(true)
//    return f.get(null).asInstanceOf[Unsafe]
//  }
//
//  private[storage] def getFieldOffset(ordinal: Int): Long =
//    baseOffset + bitSetWidthInBytes + ordinal * 8L
//
//  private def assertIndexIsValid(index: Int): Unit = {
//    assert(index >= 0, "index (" + index + ") should >= 0")
//    assert(
//      index < numFields,
//      "index (" + index + ") should < " + numberOfFields
//    )
//  }
//
//  def currentCursor: Int = currentUsedSize
//
//  /** lgh: This method is from org.apache.spark.unsafe.array.ByteArrayMethods,
//    * and is originally a static method with long as returned data type, in Java
//    * @param numBytes
//    * @return
//    */
//  private def roundNumberOfBytesToNearestWord(numBytes: Int): Int = {
//    val remainder = numBytes & 0x07; // This is equivalent to `numBytes % 8`
//    if (remainder == 0) {
//      return numBytes;
//    } else {
//      return numBytes + (8 - remainder);
//    }
//  }
//
//  /** Sets the bit at the specified index to {@code true}.
//    */
//  private def setBitMap(index: Int): Unit = {
//    //    assertIndexIsValid(index)
//    val mask = 1L << (index & 0x3f) // mod 64 and shift
//    val wordOffset = baseOffset + (index >> 6) * 8
//    val word = _UNSAFE.getLong(baseObject, wordOffset)
//    _UNSAFE.putLong(baseObject, wordOffset, word | mask)
//  }
//
//  /** Sets the bit at the specified index to {@code false}.
//    */
//  private def unsetBitMap(index: Int): Unit = {
//    //    assertIndexIsValid(index)
//    val mask = 1L << (index & 0x3f) // mod 64 and shift
//    val wordOffset = baseOffset + (index >> 6) * 8
//    val word = _UNSAFE.getLong(baseObject, wordOffset)
//    _UNSAFE.putLong(baseObject, wordOffset, word & ~mask)
//  }
//
//  private def bitMapIsSetAt(index: Int): Boolean = {
//    //    assertIndexIsValid(index)
//    val mask = 1L << (index & 0x3f) // mod 64 and shift
//    val wordOffset = baseOffset + (index >> 6) * 8
//    val word = _UNSAFE.getLong(baseObject, wordOffset)
//    return (mask & word) != 0L
//  }
//
//  /** Sets the bit at the specified index to {@code true} if it was fasle.
//    */
//  private def setBitMapIfNotSet(index: Int): Unit = {
//    //    assertIndexIsValid(index)
//    val mask = 1L << (index & 0x3f) // mod 64 and shift
//    val wordOffset = baseOffset + (index >> 6) * 8
//    val word = _UNSAFE.getLong(baseObject, wordOffset)
//    if ((mask & word) == 0L)
//      _UNSAFE.putLong(baseObject, wordOffset, word | mask)
//  }
//
//  /** Sets the bit at the specified index to {@code false} if it was true.
//    */
//  private def unsetBitMapIfSet(index: Int): Unit = {
//    assertIndexIsValid(index)
//    val mask = 1L << (index & 0x3f) // mod 64 and shift
//    val wordOffset = baseOffset + (index >> 6) * 8
//    val word = _UNSAFE.getLong(baseObject, wordOffset)
//    if ((mask & word) != 0L)
//      _UNSAFE.putLong(baseObject, wordOffset, word & ~mask)
//  }
//
//  /** lgh: This method is pted from org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder.grow(int)
//    * lgh: In this method, we first calculate the total length we need (size already used + newlyNeededSize). Then, we
//    * compare it with the already allocated size (some allocated memory may have not been used so dose not account for
//    * "currentUsedSize"). If the total size we need now exceeds the size already allocated, we will create a new backing
//    * Array[Byte] with enough memory size allocated and re-initialize this UnsafeInternalRow with the new Array[Byte].
//    *
//    * When ere is "really" a need to grow the size, the newly obtained total size will be 2 * total_size_we_neeed_now,
//    * rounded to the nearest word size ( a multiple of 8, with 8 bytes as a word ). The maximum size we can allocate to
//    * a row is ARRAY_MAX = Integer.MAX_VALUE - 15 = 2147483632 (bytes)
//    * @param newlyNeededSize
//    */
//  private[storage] def grow(newlyNeededSize: Int): Unit = {
//    val ARRAY_MAX = Integer.MAX_VALUE - 15
//    //    val ARRAY_MAX = Integer.MAX_VALUE /  2 - 63
//    if (newlyNeededSize < 0) {
//      throw new IllegalArgumentException(
//        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size is negative"
//      )
//    }
//    if (newlyNeededSize > ARRAY_MAX - currentUsedSize) {
//      throw new IllegalArgumentException(
//        "Cannot grow BufferHolder by size " + newlyNeededSize + " because the size after growing " +
//          "exceeds size limitation " + ARRAY_MAX
//      )
//    }
//    val length: Int = currentUsedSize + newlyNeededSize
//    if (sizeInBytes < length) {
//      // This will not happen frequently, because the buffer is re-used.
//      var newLength: Int = 0
//      if (length < ARRAY_MAX / 2) newLength = length * 2
//      else newLength = ARRAY_MAX
//      val roundedSize = roundNumberOfBytesToNearestWord(newLength)
//
//      val newBaseObject = null
//      val newAddress = Utils.allocateMemory(roundedSize.toLong)
//      copyMemory(
//        baseObject,
//        baseOffset,
//        newBaseObject,
//        newAddress,
//        currentUsedSize
//      )
//      _UNSAFE.freeMemory(baseOffset)
//      baseObject = newBaseObject
//      baseOffset = newAddress
//    }
//  }
//
//  private def calculateBitMapWidthInBytes(numFields: Int): Int =
//    ((numFields + 63) / 64) * 8 // lgh: this method is static in spark's Java implementation
//
//  private def calculateBitMapWidthInWords(numFields: Int): Int =
//    (numFields + 63) / 64
//
//  /** lgh: This method is from org.apache.spark.unsafe.Platform, originally a static method of Platform.
//    * lgh: This method capsulates Unsafe.copyMemory(Object, long, Object, long, long)
//    * lgh: The comment below is from org.apache.spark.unsafe.Platform
//    * Limits the number of bytes to copy per {@link Unsafe# copyMemory ( long, long, long)} to
//    * allow safepoint polling during a large copy.
//    */
//  private def copyMemory(
//      src: AnyRef,
//      srcOffset: Long,
//      dst: AnyRef,
//      dstOffset: Long,
//      length: Long
//  ): Unit = {
//    // Check if dstOffset is before or after srcOffset to determine if we should copy
//    // forward or backwards. This is necessary in case src and dst overlap.
//    var lengthRemained = length
//    var curSrcOffset = srcOffset
//    var curDstOffset = dstOffset
//
//    if (curDstOffset < curSrcOffset)
//      while (lengthRemained > 0) {
//        val size = Math.min(lengthRemained, UNSAFE_COPY_THRESHOLD)
//        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
//        lengthRemained -= size
//        curSrcOffset += size
//        curDstOffset += size
//      }
//    else {
//      curSrcOffset += lengthRemained
//      curDstOffset += lengthRemained
//      while (lengthRemained > 0) {
//        val size = Math.min(lengthRemained, UNSAFE_COPY_THRESHOLD)
//        curSrcOffset -= size
//        curDstOffset -= size
//        _UNSAFE.copyMemory(src, curSrcOffset, dst, curDstOffset, size)
//        lengthRemained -= size
//      }
//    }
//  }
//
//  override def numFields: Int = numberOfFields
//
//  override def setNullAt(i: Int): Unit = {
//    //    assertIndexIsValid(i)
//    setBitMap(i)
//    _UNSAFE.putLong(baseObject, getFieldOffset(i), 0)
//  }
//
//  def setNotNullAt(i: Int): Unit = {
//    //    assertIndexIsValid(i)
//    unsetBitMap(i)
//  }
//
//  def setNotNullIfNullAt(i: Int): Unit = {
//    //    assertIndexIsValid(i)
//    unsetBitMapIfSet(i)
//  }
//
//  /** Updates the value at column `i`. Note that after updating, the given value will be kept in this
//    * row, and the caller side should guarantee that this value won't be changed afterwards.
//    */
//  override def update(i: Int, value: Any): Unit = {
//
//    if (!value.isInstanceOf[String]) {
//      setNotNullIfNullAt(i)
//      value match {
//        case vBool: Boolean =>
//          _UNSAFE.putBoolean(baseObject, getFieldOffset(i), vBool)
//        case vByte: Byte =>
//          _UNSAFE.putByte(baseObject, getFieldOffset(i), vByte)
//        case vShort: Short =>
//          _UNSAFE.putShort(baseObject, getFieldOffset(i), vShort)
//        case vInt: Int => _UNSAFE.putInt(baseObject, getFieldOffset(i), vInt)
//        case vLong: Long =>
//          _UNSAFE.putLong(baseObject, getFieldOffset(i), vLong)
//        case vFloat: Float =>
//          _UNSAFE.putFloat(baseObject, getFieldOffset(i), vFloat)
//        case vDouble: Double =>
//          _UNSAFE.putDouble(baseObject, getFieldOffset(i), vDouble)
//        case _ =>
//      }
//    } else setString(i, value.asInstanceOf[String])
//  }
//
//  override def setBoolean(i: Int, value: Boolean): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putBoolean(baseObject, getFieldOffset(i), value)
//  }
//
//  override def setByte(i: Int, value: Byte): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putByte(baseObject, getFieldOffset(i), value)
//  }
//
//  override def setInt(i: Int, value: Int): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putInt(baseObject, getFieldOffset(i), value)
//  }
//
//  override def setLong(i: Int, value: Long): Unit =
//    _UNSAFE.putLongVolatile(baseObject, getFieldOffset(i), value)
//
//  override def setDouble(i: Int, value: Double): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putDouble(baseObject, getFieldOffset(i), value)
//  }
//
//  override def setFloat(i: Int, value: Float): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putFloat(baseObject, getFieldOffset(i), value)
//  }
//
//  override def setShort(i: Int, value: Short): Unit = {
//    setNotNullIfNullAt(i)
//    _UNSAFE.putShort(baseObject, getFieldOffset(i), value)
//  }
//
//  private[storage] def writeString(
//      i: Int,
//      byteArray: Array[Byte],
//      offset: Long,
//      size: Long
//  ): Unit = {
//    val offsetAndSize = offset << 32 | size
//    _UNSAFE.putLong(baseObject, getFieldOffset(i), offsetAndSize)
//    copyMemory(
//      byteArray,
//      BYTE_ARRAY_OFFSET,
//      baseObject,
//      baseOffset + offset,
//      size
//    )
//  }
//
//  override def setString(i: Int, vStr: String): Unit = {
//
//    //    assertIndexIsValid(i)
//    val dataByteArray = vStr.map(_.toByte).toArray
//    val stringSize = dataByteArray.length
//
//    //lgh: If the field is not null (we assume that it must be a string that has been set here), we try to put new
//    // string at the original place. If the original place does not have enough space, we will collect it for
//    // future use.
//    if (!isNullAt(i)) {
//
//      val originalOffsetAndSize: Long = getLong(i)
//      val originalSize: Int = originalOffsetAndSize.toInt
//      val originalOffset: Long = originalOffsetAndSize >>> 32
//
//      if (originalSize >= stringSize) {
//
//        setNotNullIfNullAt(i)
//        writeString(i, dataByteArray, originalOffset, stringSize.toLong)
//
//        if (originalSize - stringSize > 7) { // lgh: to avoid too many tiny fragments
//          val fragOffsetAndSize =
//            (originalOffset + stringSize) << 32 | originalSize - stringSize
//          fragmentSeq = (fragmentSeq :+ fragOffsetAndSize).sortBy(_ >>> 32)
//        }
//
//        return
//
//      } else { //originalSize < stringSize, collect it to fragmentSeq for future use
//        fragmentSeq = (fragmentSeq :+ originalOffsetAndSize).sortBy(_ >>> 32)
//      }
//
//    }
//
//    // lgh: If the program runs to here, it means that we can not put the string at "original" place. We attempt to
//    // find a fragment that can contain the string.
//    val max_idx = fragmentSeq.length - 1
//    for (idx <- 0 to max_idx) {
//      val fragmentOffset: Long = fragmentSeq(idx) >>> 32
//      val fragmentSize = fragmentSeq(idx).toInt
//      if (fragmentSize >= stringSize) {
//
//        setNotNullIfNullAt(i)
//        writeString(i, dataByteArray, fragmentOffset, stringSize.toLong)
//
//        val updatedFragOffset: Long = (fragmentOffset + stringSize) << 32
//        val updatedFragOffsetAndSize: Long =
//          updatedFragOffset | fragmentSize.toLong
//        fragmentSeq(idx) = updatedFragOffsetAndSize
//        fragmentSeq.dropWhile(_.toInt == 0)
//
//        return
//
//      }
//    }
//
//    // lgh: If the method has not returned, it means that we can not find an adequate
//    // place in the region that has been used. We try expanding our "boundary".
//    this.grow(stringSize)
//    setNotNullIfNullAt(i)
//    writeString(i, dataByteArray, currentCursor.toLong, stringSize.toLong)
//    this.currentUsedSize += stringSize
//  }
//
//  /** Make a copy of the current [[InternalRow]] object.
//    */
//  override def copy(): InternalRow = {
//    val rowCopy = new UnsafeInternalRow(numberOfFields, this.sizeInBytes)
//    rowCopy.currentUsedSize = this.currentUsedSize
//    copyMemory(
//      baseObject,
//      baseOffset,
//      rowCopy.baseObject,
//      rowCopy.baseOffset,
//      currentUsedSize
//    )
//    return rowCopy;
//  }
//
//  override def isNullAt(ordinal: Int): Boolean = bitMapIsSetAt(ordinal)
//
//  override def getBoolean(ordinal: Int): Boolean =
//    _UNSAFE.getBoolean(baseObject, getFieldOffset(ordinal))
//
//  override def getInt(ordinal: Int): Int =
//    _UNSAFE.getInt(baseObject, getFieldOffset(ordinal))
//
//  override def getLong(ordinal: Int): Long =
//    _UNSAFE.getLong(baseObject, getFieldOffset(ordinal))
//
//  override def getFloat(ordinal: Int): Float =
//    _UNSAFE.getFloat(baseObject, getFieldOffset(ordinal))
//
//  override def getDouble(ordinal: Int): Double =
//    _UNSAFE.getDouble(baseObject, getFieldOffset(ordinal))
//
//  override def getString(ordinal: Int): String = {
//    //    assertIndexIsValid(ordinal)
//    val isNUll = isNullAt(ordinal)
//    if (isNUll) return null
//    val offsetAndSize: Long = getLong(ordinal)
//    val offset: Long = (offsetAndSize >> 32).toInt
//    val size: Int = offsetAndSize.toInt
//    val buf: Array[Byte] = new Array[Byte](size)
//    copyMemory(baseObject, baseOffset + offset, buf, BYTE_ARRAY_OFFSET, size)
//    return new String(buf)
//  }
//
//  override def get(ordinal: Int, dataType: DataType): AnyRef = {
//    //    assertIndexIsValid(ordinal)
//    dataType match {
//      case BooleanType => getBoolean(ordinal).asInstanceOf[AnyRef]
//      case IntegerType => getInt(ordinal).asInstanceOf[AnyRef]
//      case LongType    => getLong(ordinal).asInstanceOf[AnyRef]
//      case FloatType   => getFloat(ordinal).asInstanceOf[AnyRef]
//      case DoubleType  => getDouble(ordinal).asInstanceOf[AnyRef]
//      case StringType  => getString(ordinal).asInstanceOf[AnyRef]
//      case _           => getLong(ordinal).asInstanceOf[AnyRef]
//      //    case AnyDataType =>
//      //    case NumericType =>
//    }
//  }
//
//  override def toString: String = {
//    val build = new StringBuilder("[")
//    var i = 0
//    while (i < sizeInBytes) {
//      if (i != 0) build.append(',')
//      build.append(
//        java.lang.Long.toHexString(_UNSAFE.getLong(baseObject, baseOffset + i))
//      )
//
//      i += 8
//    }
//    build.append(']')
//    build.toString
//  }
//
//  private[storage] def show(schema: StructType): Unit = {
//    val build = new StringBuilder("[")
//    val fields = schema.fields
//    for (ordinal <- fields.indices) {
//      if (ordinal != 0) build.append(",")
//      val dataType = fields(ordinal).dataType
//      val value: Any = if (isNullAt(ordinal)) {
//        "null"
//      } else {
//        dataType match {
//          case BooleanType => getBoolean(ordinal)
//          case IntegerType => getInt(ordinal)
//          case LongType    => getLong(ordinal)
//          case FloatType   => getFloat(ordinal)
//          case DoubleType  => getDouble(ordinal)
//          case StringType  => getString(ordinal)
//          case _           => "unSupportType"
//        }
//      }
//      build.append(value.toString)
//    }
//    build.append("]")
//    println(build.toString)
//  }
//
//  override def hashCode: Int =
//    Murmur3_x86_32.hashUnsafeWords(baseObject, baseOffset, sizeInBytes, 42)
//
//  override def equals(other: Any): Boolean = {
//    other match {
//      case o: UnsafeInternalRow =>
//        return (sizeInBytes == o.sizeInBytes) && ByteArrayMethods.arrayEquals(
//          baseObject,
//          baseOffset,
//          o.baseObject,
//          o.baseOffset,
//          sizeInBytes
//        )
//      case _ =>
//    }
//    false
//  }
//
//  override def finalize(): Unit = {
//    _UNSAFE.freeMemory(baseOffset)
//    super.finalize()
//  }
//
//}
//
