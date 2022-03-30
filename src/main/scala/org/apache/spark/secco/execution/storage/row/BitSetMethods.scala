/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.secco.execution.storage.row

//package org.apache.spark.unsafe.bitset

//import org.apache.spark.unsafe._UNSAFE


/**
  * Methods for working with fixed-size uncompressed bitsets.
  *
  * We assume that the bitset data is word-aligned (that is, a multiple of 8 bytes in length).
  *
  * Each bit occupies exactly one bit of storage.
  */
object BitSetMethods {
  
  private val _UNSAFE = UnsafeInternalRow._UNSAFE
  
  private val WORD_SIZE = 8

  /**
    * Sets the bit at the specified index to {@code true}.
    */
  def set(baseObject: Any, baseOffset: Long, index: Int): Unit = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    val mask = 1L << (index & 0x3f) // mod 64 and shift
    val wordOffset = baseOffset + (index >> 6) * WORD_SIZE
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    _UNSAFE.putLong(baseObject, wordOffset, word | mask)
  }

  /**
    * Sets the bit at the specified index to {@code false}.
    */
  def unset(baseObject: Any, baseOffset: Long, index: Int): Unit = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    val mask = 1L << (index & 0x3f)
    val wordOffset = baseOffset + (index >> 6) * WORD_SIZE
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    _UNSAFE.putLong(baseObject, wordOffset, word & ~mask)
  }

  /**
    * Returns {@code true} if the bit is set at the specified index.
    */
  def isSet(baseObject: Any, baseOffset: Long, index: Int): Boolean = {
    assert(index >= 0, "index (" + index + ") should >= 0")
    val mask = 1L << (index & 0x3f)
    val wordOffset = baseOffset + (index >> 6) * WORD_SIZE
    val word = _UNSAFE.getLong(baseObject, wordOffset)
    (word & mask) != 0
  }

  /**
    * Returns {@code true} if any bit is set.
    */
  def anySet(baseObject: Any, baseOffset: Long, bitSetWidthInWords: Long): Boolean = {
    var addr = baseOffset
    var i = 0
    while ( {
      i < bitSetWidthInWords
    }) {
      if (_UNSAFE.getLong(baseObject, addr) != 0) return true

      i += 1
      addr += WORD_SIZE
    }
    false
  }

  /**
    * Returns the index of the first bit that is set to true that occurs on or after the
    * specified starting index. If no such bit exists then {@code -1} is returned.
    * <p>
    * To iterate over the true bits in a BitSet, use the following loop:
    * <pre>
    * <code>
    * for (long i = bs.nextSetBit(0, sizeInWords); i &gt;= 0;
    * i = bs.nextSetBit(i + 1, sizeInWords)) {
    * // operate on index i here
    * }
    * </code>
    * </pre>
    *
    * @param fromIndex         the index to start checking from (inclusive)
    * @param bitsetSizeInWords the size of the bitset, measured in 8-byte words
    * @return the index of the next set bit, or -1 if there is no such bit
    */
  def nextSetBit(baseObject: Any, baseOffset: Long, fromIndex: Int, bitsetSizeInWords: Int): Int = {
    var wi = fromIndex >> 6
    if (wi >= bitsetSizeInWords) return -1
    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = _UNSAFE.getLong(baseObject, baseOffset + wi * WORD_SIZE) >> subIndex
    if (word != 0) return (wi << 6) + subIndex + java.lang.Long.numberOfTrailingZeros(word)
    // Find the next set bit in the rest of the words
    wi += 1
    while ( {
      wi < bitsetSizeInWords
    }) {
      word = _UNSAFE.getLong(baseObject, baseOffset + wi * WORD_SIZE)
      if (word != 0) return (wi << 6) + java.lang.Long.numberOfTrailingZeros(word)
      wi += 1
    }
    -1
  }
}

//final class BitSetMethods private() // Make the default constructor private, since this only holds static methods.
//{
//}
