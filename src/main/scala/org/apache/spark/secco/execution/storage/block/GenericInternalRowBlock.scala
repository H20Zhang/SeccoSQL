package org.apache.spark.secco.execution.storage.block

import org.apache.spark.Partitioner
import org.apache.spark.secco.execution.storage.row.{
  GenericInternalRow,
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.types._

import scala.collection.mutable.ArrayBuffer

/** The InternalBlock backed by GenericInternalRows */
class GenericInternalRowBlock extends InternalBlock with RowLike {
  private var rows: Array[InternalRow] = _
  private var blockSchema: StructType = _
  private var dictionaryOrder: Option[Seq[String]] = None

  def this(rows: Array[InternalRow], blockSchema: StructType) {
    this()
    this.rows = rows
    this.blockSchema = blockSchema
  }

  override def iterator: Iterator[InternalRow] = rows.toIterator

  override def size(): Long = rows.length

  override def isEmpty(): Boolean = rows.isEmpty

  override def nonEmpty(): Boolean = rows.nonEmpty

  override def schema(): StructType = blockSchema

  override def sortBy(DictionaryOrder: Seq[String]): InternalBlock = {
    val priority =
      new Array[Int](
        DictionaryOrder.length
      ) // store the index of the field that needs to be sorted
    this.dictionaryOrder = Option(DictionaryOrder)
    val unsortedRows = rows.clone
    var gotIndex = 0
    for (i <- blockSchema.fields.indices) {
      val name = blockSchema.fields(i).name
      if (DictionaryOrder.contains(name)) {
        priority(gotIndex) = i
        gotIndex += 1
      }
    }
    quickSort(unsortedRows, priority, 0, size().toInt) // sort the unsortedRows
    val newGenericInternalBlock =
      GenericInternalRowBlock.apply(unsortedRows, blockSchema)
    newGenericInternalBlock.setDictionaryOrder(Option(DictionaryOrder))
    newGenericInternalBlock
  }

  private def setDictionaryOrder(dictionaryOrder: Option[Seq[String]]): Unit = {
    this.dictionaryOrder = dictionaryOrder
  }
  override def partitionBy(partitioner: Partitioner): Array[InternalBlock] = ???

  override def show(num: Int): Unit = {
    val showRows = rows.take(num)
    for (row <- showRows) {
      if (row.isInstanceOf[GenericInternalRow]) println(row)
      else row.asInstanceOf[UnsafeInternalRow].show(blockSchema)
    }
  }

  override def toArray(): Array[InternalRow] = {
    val newRows = new Array[InternalRow](size().toInt)
    for (i <- rows.indices) {
      newRows(i) = this.getRow(i)
    }
    newRows
  }

  override def getRow(i: Int): InternalRow = rows(i).copy()

  override def merge(
      other: InternalBlock,
      maintainSortOrder: Boolean
  ): InternalBlock = {
    val source_size = size().toInt
    val target_size = other.size().toInt
    val otherIterator = other.iterator
    var sortedRows = new Array[InternalRow](source_size + target_size)
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

      // merge using double pointer algorithm
      var p1 = 0
      var p2 = 0
      var tmpP2 = p2
      var otherRow = otherIterator.next().copy()

      val sortOrder = dictionaryOrder.get
      val priority = new Array[Int](sortOrder.size)
      for (index <- sortOrder.indices) {
        val fieldName = sortOrder(index)
        val field = schema().fields.filter(_.name == fieldName)(0)
        val indexInRow = schema().fields.indexOf(field)
        priority(index) = indexInRow
      }
      var cur: InternalRow = null // initial value doesn't matter here
      while (p1 < source_size || p2 < target_size) {
        if (p2 > tmpP2 && p2 < target_size) {
          otherRow = otherIterator.next().copy()
          tmpP2 = p2
        }
        if (p1 == source_size) {
          cur = otherRow
          p2 += 1
        } else if (p2 == target_size) {
          cur = rows(p1).copy()
          p1 += 1
        } else if (mergeSmallerThan(priority, p1, p2, otherRow)) {
          cur = rows(p1).copy()
          p1 += 1
        } else {
          cur = otherRow
          p2 += 1
        }
        sortedRows(p1 + p2 - 1) = cur
      }
    } else {
      // No sort here, just concat
      sortedRows = Array.concat(this.toArray(), other.toArray())
    }
    val mergedGenericInternalBlock =
      GenericInternalRowBlock.apply(sortedRows, blockSchema)
    mergedGenericInternalBlock
  }

  override def clone(): GenericInternalRowBlock = {
    val clonedRows = new Array[InternalRow](size().toInt)
    for (i <- rows.indices) {
      clonedRows(i) = this.getRow(i)
    }
    GenericInternalRowBlock.apply(clonedRows, blockSchema)
  }

  private def quickSort(
      unsortedRows: Array[InternalRow],
      priority: Array[Int],
      left: Int,
      right: Int
  ): Unit = {
    if (left < right) {
      val partitionIndex = partition(unsortedRows, priority, left, right)
      quickSort(unsortedRows, priority, left, partitionIndex - 1)
      quickSort(unsortedRows, priority, partitionIndex + 1, right)
    }
  }

  private def partition(
      unsortedRows: Array[InternalRow],
      priority: Array[Int],
      left: Int,
      right: Int
  ): Int = {
    val pivot = left
    var j = pivot + 1
    var i = j
    val firstDataType = blockSchema.fields(priority(0)).dataType
    val firstOrdinal = priority(0)
    firstDataType match {
      case DoubleType =>
        while (i < right) {
          val value_i = unsortedRows(i).getDouble(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getDouble(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case FloatType =>
        while (i < right) {
          val value_i = unsortedRows(i).getFloat(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getFloat(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case IntegerType =>
        while (i < right) {
          val value_i = unsortedRows(i).getInt(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getInt(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case LongType =>
        while (i < right) {
          val value_i = unsortedRows(i).getLong(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getLong(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case StringType =>
        while (i < right) {
          val value_i = unsortedRows(i).getString(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getString(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case BooleanType =>
        while (i < right) {
          val value_i = unsortedRows(i).getBoolean(firstOrdinal)
          val value_pivot = unsortedRows(pivot).getBoolean(firstOrdinal)
          if (value_i < value_pivot) {
            swap(unsortedRows, i, j)
            j += 1
          } else if (value_i == value_pivot) {
            if (smallerThan(unsortedRows, priority.drop(1), i, pivot)) {
              swap(unsortedRows, i, j)
              j += 1
            }
          }
          i += 1
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    swap(unsortedRows, pivot, j - 1)
    j - 1
  }

  private def smallerThan(
      unsortedRows: Array[InternalRow],
      priority: Array[Int],
      i: Int,
      j: Int,
      index: Int = 0
  ): Boolean = {
    if (index == priority.length) return false // comparison ends
    val castType = schema().fields(priority(index)).dataType
    //    var newj = j
    val ordinal = priority(index)
    castType match {
      case DoubleType =>
        val value_i = unsortedRows(i).getDouble(ordinal)
        val value_j = unsortedRows(j).getDouble(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case FloatType =>
        val value_i = unsortedRows(i).getFloat(ordinal)
        val value_j = unsortedRows(j).getFloat(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case IntegerType =>
        val value_i = unsortedRows(i).getInt(ordinal)
        val value_j = unsortedRows(j).getInt(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case LongType =>
        val value_i = unsortedRows(i).getLong(ordinal)
        val value_j = unsortedRows(j).getLong(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case StringType =>
        val value_i = unsortedRows(i).getString(ordinal)
        val value_j = unsortedRows(j).getString(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case BooleanType =>
        val value_i = unsortedRows(i).getBoolean(ordinal)
        val value_j = unsortedRows(j).getBoolean(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return smallerThan(unsortedRows, priority, i, j, index + 1)
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    false
  }

  /** This method is used for merge
    *
    * @param priority : it stores the indexInRow by the order of the fields that need to be considered during sorting.
    * @param i        : the index of the row in source block
    * @param j        : the index of the row in target block
    * @param other    : the target block
    * @param index    : the index of the field that needs to be focused on now
    * @return a boolean that indicates whether the source row is smaller than the target row
    */
  private def mergeSmallerThan(
      priority: Array[Int],
      i: Int,
      j: Int,
      otherRow: InternalRow,
      index: Int = 0
  ): Boolean = {
    if (index == priority.length) return false // comparison ends
    val castType = schema().fields(priority(index)).dataType
    val ordinal = priority(index)
    val sourceRow = rows(i)
    castType match {
      case DoubleType =>
        val value_i = sourceRow.getDouble(ordinal)
        val value_j = otherRow.getDouble(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case FloatType =>
        val value_i = sourceRow.getFloat(ordinal)
        val value_j = otherRow.getFloat(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case IntegerType =>
        val value_i = sourceRow.getInt(ordinal)
        val value_j = otherRow.getInt(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case LongType =>
        val value_i = sourceRow.getLong(ordinal)
        val value_j = otherRow.getLong(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case StringType =>
        val value_i = sourceRow.getString(ordinal)
        val value_j = otherRow.getString(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          return mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case BooleanType =>
        val value_i = sourceRow.getBoolean(ordinal)
        val value_j = otherRow.getBoolean(ordinal)
        if (value_i < value_j) {
          return true
        } else if (value_i == value_j) {
          mergeSmallerThan(priority, i, j, otherRow, index + 1)
        }
      case _ =>
        throw new Exception("Comparison uses dataType that is not allowed!")
    }
    false
  }

  private def swap(rows: Array[InternalRow], i: Int, j: Int): Unit = {
    val tmp = rows(j)
    rows(j) = rows(i)
    rows(i) = tmp
  }

  override def getDictionaryOrder: Option[Seq[String]] = dictionaryOrder
}

object GenericInternalRowBlock {

  /** Initialize the [[GenericInternalRowBlock]] by array of [[InternalRow]] */
  def apply(
      rows: Array[InternalRow],
      schema: StructType
  ): GenericInternalRowBlock = {
    val builder = new GenericInternalRowBlockBuilder(schema)
    for (row <- rows) {
      builder.add(row)
    }
    builder.build()
  }

  /** Return the builder for building [[GenericInternalRowBlock]] */
  def builder(schema: StructType): GenericInternalRowBlockBuilder =
    new GenericInternalRowBlockBuilder(schema)
}

class GenericInternalRowBlockBuilder(schema: StructType)
    extends InternalBlockBuilder {
  private var rows: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()

  override def add(row: InternalRow): Unit = rows.append(row.copy())

  override def build(): GenericInternalRowBlock =
    new GenericInternalRowBlock(rows.toArray, schema)
}
