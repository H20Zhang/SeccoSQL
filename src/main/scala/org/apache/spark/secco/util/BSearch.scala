package org.apache.spark.secco.util

import org.apache.spark.secco.execution.storage.Utils

object BSearch {
  def search[T](array: Array[T], value: T)(implicit
      arithmetic: Numeric[T]
  ): Int = {
    var left: Int = 0;
    var right: Int = array.length - 1;
    while (right > left) {
      val mid = left + (right - left) / 2
      val comp = arithmetic.compare(array(mid), value)
      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def search(
      array: Array[Double],
      value: Double,
      _leftPos: Int,
      _rightPos: Int
  ): Int = {
    var left: Int = _leftPos;
    var right: Int = _rightPos;
    while (right > left) {
      val mid = left + (right - left) / 2
      val midVal = array(mid)

      var comp = 0
      if (midVal > value) {
        comp = 1
      } else if (midVal < value) {
        comp = -1
      }

      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def searchUnsafe(
      address: Long,
      value: Long,
      _leftPos: Int,
      _rightPos: Int,
      _elemSize: Int
  ): Int = {
    assert(
      _elemSize == 8 || _elemSize == 16,
      "Currently only _elemSize of 8 or 16 is supported."
    )

    var left: Int = _leftPos;
    var right: Int = _rightPos;
    while (right > left) {
      val mid = left + (right - left) / 2
      val midVal = Utils._UNSAFE.getLong(address + mid * _elemSize)

      var comp = 0
      if (midVal > value) {
        comp = 1
      } else if (midVal < value) {
        comp = -1
      }

      if (comp == 0)
        return mid; //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  def searchUnsafeString(
      address: Long,
      variableLengthZoneAddress: Long,
      value: String,
      _leftPos: Int,
      _rightPos: Int,
      _elemSize: Int
  ): Int = {

    assert(
      _elemSize == 8 || _elemSize == 16,
      "Currently only _elemSize of 8 or 16 is supported."
    )

    var left: Int = _leftPos;
    var right: Int = _rightPos;
    while (right > left) {
      val mid = left + (right - left) / 2
      val midOffsetAndSize = Utils._UNSAFE.getLong(address + mid * _elemSize)
      val midStringOffset: Long = midOffsetAndSize >> 32
      val midStringSize: Int = midOffsetAndSize.toInt

      var comp = 0
      val lengthLimit = Math.min(value.length, midStringSize)
      var i = 0
      while (comp == 0 && i < lengthLimit) {
        val tempChar =
          Utils._UNSAFE.getChar(variableLengthZoneAddress + midOffsetAndSize)
        if (tempChar > value(i)) comp = 1
        else if (tempChar < value(i)) comp = -1
        i += 1
      }

      if (comp == 0) {
        if (midStringSize == value.length) return mid
        else if (midStringSize > value.length) right = mid
        else left = mid
      }
      //negative if test < value
      else if (comp > 0) //list(mid) > value
        right = mid;
      else if (comp < 0) //list(mid) < value
        left = mid + 1;
    }
    -1;
  }

  //  def searchFather (    address: Long,
  //                          value: Int,
  //                          _leftPos: Int,
  //                          _rightPos: Int,
  //                          _elemSize: Int
  //                        ): Int = {
  //
  //    assert(_elemSize == 8 || _elemSize == 16, "Currently only _elemSize of 8 or 16 is supported.")
  //
  //    var left: Int = _leftPos;
  //    var right: Int = _rightPos;
  //    while (right > left) {
  //      val mid = left + (right - left) / 2
  //      val midOffsetAndSize = Utils._UNSAFE.getLong(address + mid * 8)
  //      val midStringOffset: Long = midOffsetAndSize >> 32
  //      val midStringSize: Int = midOffsetAndSize.toInt
  //
  //      var comp = 0
  //      val lengthLimit = Math.min(value.length, midStringSize)
  //      var i = 0
  //      while(comp == 0 && i < lengthLimit){
  //        val tempChar = Utils._UNSAFE.getChar(variableLengthZoneAddress + midOffsetAndSize)
  //        if(tempChar > value(i)) comp = 1
  //        else if(tempChar < value(i)) comp = -1
  //        i += 1
  //      }
  //
  //      if(comp == 0){
  //        if(midStringSize == value.length) return mid
  //        else if(midStringSize > value.length) right = mid
  //        else left = mid
  //      }
  //      //negative if test < value
  //      else if (comp > 0) //list(mid) > value
  //        right = mid;
  //      else if (comp < 0) //list(mid) < value
  //        left = mid + 1;
  //    }
  //    -1;
  //  }
}
