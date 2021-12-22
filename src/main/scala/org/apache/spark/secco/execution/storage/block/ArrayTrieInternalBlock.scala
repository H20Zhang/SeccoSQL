package org.apache.spark.secco.execution.storage.block

import java.util.Comparator
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap
import org.apache.spark.secco.execution.storage.collection.ArraySegment
import org.apache.spark.secco.util.BSearch

import scala.collection.mutable.ArrayBuffer


// edge:Array[(ID, ID, Value)], node:Array[(Start, End)]
class ArrayTrieInternalBlock(
                              neighbors: Array[Int],
                              val values: Array[Double],
                              neighborBegins: Array[Int],
                              neighborEnds: Array[Int],
                              level: Int
                            )  {
  self =>

  private var rootBegin = neighborBegins(0)
  private var rootEnd = neighborEnds(0)
  private val emptyArray = ArraySegment.emptyArraySegment
  private var prevLevel = 0
  private var prevBegin = rootBegin
  private var prevEnd = rootEnd

  private lazy val rootLevelMap = {

    //fastuil.HashMap
    val tempMap = new Double2IntOpenHashMap((rootEnd - rootBegin))

    var i = rootBegin
    while (i < rootEnd) {
      tempMap.put(values(i), i)
      i += 1
    }

    tempMap.trim()
    tempMap
  }

  def get(key: ArraySegment, outputBuffer: ArraySegment): Unit = {

    var start = rootBegin
    var end = rootEnd
    var id = 0
    var i = 0
    val bindingLevel = key.size
    var pos = 0

    while (i < bindingLevel) {

      if (i == 0) {
        pos = rootLevelMap.getOrDefault(key(i), -1)
      } else {
        pos = BSearch.search(values, key(i), start, end)
      }

      if (pos == -1) {
        outputBuffer.array = values
        outputBuffer.begin = start
        outputBuffer.end = start
        outputBuffer.size = 0
        return emptyArray
      }

      id = neighbors(pos)
      start = neighborBegins(id)
      end = neighborEnds(id)
      i += 1
    }

    outputBuffer.array = values
    outputBuffer.begin = start
    outputBuffer.end = end
    outputBuffer.size = end - start
  }

  def contains(key: Array[Double]): Boolean = {

    var start = rootBegin
    var end = rootEnd
    var id = 0
    var i = 0
    val bindingLevel = key.size
    var pos = 0

    while (i < bindingLevel) {

      if (i == 0) {
        pos = rootLevelMap.getOrDefault(key(i), -1)
      } else {
        pos = BSearch.search(values, key(i), start, end)
      }

      if (pos == -1) {
        return false
      }

      if (i < (level - 1)) {
        id = neighbors(pos)
        start = neighborBegins(id)
        end = neighborEnds(id)
      }
      i += 1
    }

    true
  }

  def get(key: ArraySegment): ArraySegment = {
    val arraySegment = new ArraySegment(null, 0, 0, 0)
    get(key, arraySegment)

    arraySegment
  }


  def toArray(): Array[Array[Double]] = {
    var tables =
      get(ArraySegment.emptyArray()).toArray().map(f => Array(f))

    var i = 1
    while (i < level) {
      tables = tables.flatMap { f =>
        val nextLevelValues = get(ArraySegment(f))
        nextLevelValues.toArray().map(value => f :+ value)
      }

      i += 1
    }

    tables
  }

  override def toString: String = {
    s"""
       |== Array Trie ==
       |neighbors:${neighbors.toSeq}
       |values:${values.toSeq}
       |neighborStart:${neighborBegins.toSeq}
       |neighborEnd:${neighborEnds.toSeq}
     """.stripMargin
  }

}

// Scan the tuples of the relation sequentially, for each tuple,
// locate the bit where it firstly diverge from the previous tuple and then create a new trie node.
object ArrayTrieInternalBlock {
  def apply(table: Array[Array[Double]], arity: Int): ArrayTrieInternalBlock = {

    //sort the relation in lexical order
    val comparator = new LexicalOrderComparator(arity)
    java.util.Arrays.sort(table, comparator)

    //init
    var idCounter = 0
    var leafIDCounter = -1
    val prevIDs = new Array[Int](arity)
    var prevTuple = new Array[Double](arity)
    //    val edgeBuffer = ArrayBuffer[(Int, Int, DataType)]()
    val edgeBuffer = ArrayBuffer[ValuedEdge]()
    val nodeBuffer = ArrayBuffer[(Int, Int)]()
    val tableSize = table.size

    var i = 0
    while (i < arity) {
      prevIDs(i) = 0
      prevTuple(i) = Int.MaxValue
      i += 1
    }

    //construct edges for ArrayTrie
    val rootID = idCounter
    idCounter += 1

    i = 0
    while (i < tableSize) {
      val curTuple = table(i)

      //find the j-th position where value of curTuple diverge from prevTuple
      var diffPos = -1
      var isConsecutive = true
      var j = 0
      while (j < arity) {
        if (curTuple(j) != prevTuple(j) && isConsecutive == true) {
          diffPos = j
          isConsecutive = false
        }
        j += 1
      }

      //deal with the case, where curTuple is the as prevTuple
      if (isConsecutive) {
        diffPos = arity - 2
      } else {
        diffPos = diffPos - 1
      }

      //for each value of curTuple diverge from preTuple, create a new NodeID
      //create edges between NodeIDs
      while (diffPos < arity - 1) {
        val nextPos = diffPos + 1
        var prevID = 0

        if (diffPos == -1) {
          prevID = rootID
        } else {
          prevID = prevIDs(diffPos)
        }

        var newID = 0

        if (diffPos < arity - 2) {
          newID = idCounter
          idCounter += 1
        } else {
          newID = leafIDCounter
          leafIDCounter -= 1
        }

        //create edges between NodeIDs
        //        edgeBuffer += ((prevID, newID, curTuple(nextPos)))
        edgeBuffer += ValuedEdge(prevID, newID, curTuple(nextPos))
        prevIDs(nextPos) = newID

        diffPos += 1
      }

      prevTuple = curTuple
      i += 1
    }

    //add a tuple to mark the end of the edges
    //    edgeBuffer += ((Int.MaxValue, Int.MaxValue, Int.MaxValue))
    edgeBuffer += ValuedEdge(Int.MaxValue, Int.MaxValue, Int.MaxValue)

    //    val x = mutable.ArrayBuilder()
    //sort edges first by "id" then "value"
    val edges = edgeBuffer.toArray
    val edgeComparator = new ValuedEdgeComparator
    java.util.Arrays.sort(edges, edgeComparator)

    //construct node for ArrayTrie
    //  scan the sorted edges
    i = 0
    var start = 0
    var end = 0
    var currentValue = 0

    while (i < edges.size) {
      if (edges(i).u == currentValue) {
        end += 1
      } else {
        nodeBuffer += ((start, end))

        currentValue = edges(i).u
        start = i
        end = i + 1
      }
      i += 1
    }

    val neighbors = new Array[Int](edges.size - 1)
    val values = new Array[Double](edges.size - 1)
    val neighborsBegin = new Array[Int](nodeBuffer.size)
    val neighborsEnd = new Array[Int](nodeBuffer.size)

    i = 0
    val edgeSize = edges.size - 1
    while (i < edgeSize) {
      val edge = edges(i)
      neighbors(i) = edge.v
      values(i) = edge.value
      i += 1
    }

    i = 0
    val nodesSize = nodeBuffer.size
    while (i < nodesSize) {
      val node = nodeBuffer(i)
      neighborsBegin(i) = node._1
      neighborsEnd(i) = node._2
      i += 1
    }

    new ArrayTrieInternalBlock(neighbors, values, neighborsBegin, neighborsEnd, arity)
  }
}

class LexicalOrderComparator(attrNum: Int)
  extends Comparator[Array[Double]]
    with Serializable {

  override def compare(
                        o1: Array[Double],
                        o2: Array[Double]
                      ): Int = {
    var i = 0
    while (i < attrNum) {
      if (o1(i) < o2(i)) {
        return -1
      } else if (o1(i) > o2(i)) {
        return 1
      } else {
        i += 1
      }
    }
    return 0
  }
}

class TupleEdgeComparator
  extends Comparator[(Int, Int, Double)]
    with Serializable {

  override def compare(
                        o1: (Int, Int, Double),
                        o2: (Int, Int, Double)
                      ): Int = {

    if (o1._1 < o2._1) {
      return -1
    } else if (o1._1 > o2._1) {
      return 1
    } else {
      if (o1._3 < o2._3) {
        return -1
      } else if (o1._3 > o2._3) {
        return 1
      } else return 0
    }
  }
}

case class ValuedEdge(u: Int, v: Int, value: Double)

class ValuedEdgeComparator extends Comparator[ValuedEdge] with Serializable {

  override def compare(o1: ValuedEdge, o2: ValuedEdge): Int = {

    if (o1.u < o2.u) {
      return -1
    } else if (o1.u > o2.u) {
      return 1
    } else {
      if (o1.value < o2.value) {
        return -1
      } else if (o1.value > o2.value) {
        return 1
      } else return 0
    }
  }
}
