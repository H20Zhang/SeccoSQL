package org.apache.spark.secco.execution.plan.computation.utils

import java.util.Comparator

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap
import org.apache.spark.secco.execution.{InternalDataType, InternalRow}
import org.apache.spark.secco.execution.plan.computation.iter.{
  EmptyIterator,
  SingularIterator
}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** This trait defines accessor used to access potentially shared data to avoid conflict problems in multi-threading */
trait Accessor {
  def iterator(): Iterator[InternalRow]
  def indexIterator(prefix: InternalRow): Iterator[InternalRow]
}

/** This trait defines methods to be supported by a Trie */
trait Trie extends Serializable {

  /** Return accessor for accessing the trie */
  def access(): Accessor

  /** Check if the trie contains prefix */
  def contains(prefix: InternalRow): Boolean

  /** Return the values of next level based on prefix */
  def nextLevel(binding: ArraySegment): ArraySegment

  /** Return the values of next level based on prefix by outputArraySegment */
  def nextLevelWithAdjust(
      binding: ArraySegment,
      outputArraySegment: ArraySegment
  ): Unit

  /** Return the values of next level based on prefix by outputArraySegment */
  def nextLevel(binding: ArraySegment, outputArraySegment: ArraySegment): Unit

  /** Return internal rows represented in this Trie */
  def toInternalRows(): Array[InternalRow]
}

object Trie {

  /** Instantiate a trie, currently [[org.apache.spark.secco.execution.plan.computation.utils.ArrayTrie]], from internal rows */
  def apply(rows: Array[InternalRow], arity: Int): Trie = {
    ArrayTrie(rows, arity)
  }
}

// edge:Array[(ID, ID, Value)], node:Array[(Start, End)]
class ArrayTrie(
    neighbors: Array[Int],
    val values: Array[InternalDataType],
    neighborBegins: Array[Int],
    neighborEnds: Array[Int],
    level: Int
) extends Trie {

  self =>

  var rootBegin = neighborBegins(0)
  var rootEnd = neighborEnds(0)
  val emptyArray = ArraySegment.emptyArraySegment
  var prevLevel = 0
  var prevBegin = rootBegin
  var prevEnd = rootEnd

  lazy val rootLevelMap = {

    //fastuil.HashMap
    val tempMap = new Double2IntOpenHashMap((rootEnd - rootBegin))

    //trove.HashMap
//    val tempMap = new TLongIntHashMap((rootEnd - rootBegin), 0.5f, -1, -1)

    var i = rootBegin
    while (i < rootEnd) {
      tempMap.put(values(i), i)
      i += 1
    }

    tempMap.trim()
//    tempMap.compact()
    tempMap
  }

  override def nextLevel(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = {

    var start = rootBegin
    var end = rootEnd
//    var start = 0
//    var end = 0
    var id = 0
    var i = 0
    val bindingLevel = binding.size
//    start = prevBegin
//    end = prevEnd
    var pos = 0

    while (i < bindingLevel) {

      if (i == 0) {
//        pos = rootLevelMap.get(binding(i))
        pos = rootLevelMap.getOrDefault(binding(i), -1)
      } else {
        pos = BSearch.search(values, binding(i), start, end)
      }

      if (pos == -1) {
        inputArraySegment.array = values
        inputArraySegment.begin = start
        inputArraySegment.end = start
        inputArraySegment.size = 0
        return emptyArray
      }

      id = neighbors(pos)
      start = neighborBegins(id)
      end = neighborEnds(id)
      i += 1
    }

    inputArraySegment.array = values
    inputArraySegment.begin = start
    inputArraySegment.end = end
    inputArraySegment.size = end - start
  }

  override def contains(prefix: InternalRow): Boolean = {

    var start = rootBegin
    var end = rootEnd
    //    var start = 0
    //    var end = 0
    var id = 0
    var i = 0
    val bindingLevel = prefix.size
    //    start = prevBegin
    //    end = prevEnd
    var pos = 0

    while (i < bindingLevel) {

      if (i == 0) {
        //        pos = rootLevelMap.get(binding(i))
        pos = rootLevelMap.getOrDefault(prefix(i), -1)
      } else {
        pos = BSearch.search(values, prefix(i), start, end)
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

  def nextLevel(binding: ArraySegment): ArraySegment = {
    val arraySegment = new ArraySegment(null, 0, 0, 0)
    nextLevel(binding, arraySegment)

    arraySegment
  }

  override def nextLevelWithAdjust(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = {

    var id = 0
    var start = rootBegin
    var end = rootEnd
    val bindingLevel = binding.size
    var i = 0
    var pos = 0

    if (prevLevel < bindingLevel) {
      i = prevLevel
      start = prevBegin
      end = prevEnd
    }

    while (i < bindingLevel) {

      if (i == 0) {
        pos = rootLevelMap.getOrDefault(binding(i), -1)
      } else {
        pos = BSearch.search(values, binding(i), start, end)
      }

      if (pos == -1) {
        inputArraySegment.array = values
        inputArraySegment.begin = start
        inputArraySegment.end = start
        inputArraySegment.size = 0
        return inputArraySegment
      }

      id = neighbors(pos)
      start = neighborBegins(id)
      end = neighborEnds(id)
      i += 1
    }

    prevLevel = i
    prevBegin = start
    prevEnd = end

//    inputArraySegment.set(values, start, end, end - start)

    inputArraySegment.array = values
    inputArraySegment.begin = start
    inputArraySegment.end = end
    inputArraySegment.size = end - start
  }

  //just for verify the correctness of the trie implementation
  def toInternalRows(): Array[Array[InternalDataType]] = {
    var tables =
      nextLevel(ArraySegment.emptyArray()).toArray().map(f => Array(f))

    var i = 1
    while (i < level) {
      tables = tables.flatMap { f =>
        val nextLevelValues = nextLevel(ArraySegment(f))
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

  override def access(): Accessor =
    new Accessor {

      lazy val pesudoAttrOrder = Range(0, level).map(_.toString)
      lazy val plf =
        PartialLeapFrogJoin(
          Array(self),
          pesudoAttrOrder,
          Seq(pesudoAttrOrder)
        )
      private lazy val singularIt = new SingularIterator
      private lazy val emptyIt = new EmptyIterator

      override def indexIterator(prefix: InternalRow): Iterator[InternalRow] = {

        // if zero length prefix is input, it should return full iterator
        if (prefix.size == 0) {
          iterator()
        } else if (prefix.size == level) {
          if (contains(prefix)) {
            singularIt.reset(prefix)
          } else {
            emptyIt
          }
        } else {
          if (contains(prefix)) {
            plf.unsafeInit(prefix)
          } else {
            emptyIt
          }
        }
      }

      override def iterator(): Iterator[InternalRow] = {
        val pesudoAttrOrder = Range(0, level).map(_.toString)
        val lf =
          LeapFrogJoin(Array(self), pesudoAttrOrder, Seq(pesudoAttrOrder))

        lf

      }
    }
}

class GraphTrie(graph: mutable.HashMap[InternalDataType, ArraySegment])
    extends Trie {

  val rootLevel = ArraySegment(graph.keys.toArray.sorted)
  val emptyArray = ArraySegment.emptyArraySegment

  def nextLevel(binding: ArraySegment): ArraySegment = {
    val level = binding.size

    if (level == 0) {
      rootLevel
    } else {
      val bind = binding(0)
      if (graph.contains(bind)) {
        graph(bind)
      } else {
        emptyArray
      }
    }
  }

  override def toInternalRows(): Array[Array[InternalDataType]] = {
    graph.toArray.flatMap(f => f._2.array.map(g => Array(f._1, g)))
  }

  override def nextLevel(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = ???

  override def nextLevelWithAdjust(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = ???

  override def contains(prefix: InternalRow): Boolean = ???

  override def access(): Accessor = ???
}

object GraphTrie {
  def apply(table: Array[Array[InternalDataType]], arity: Int): GraphTrie = {
    assert(arity == 2)

    val graphBuffer =
      mutable.HashMap[InternalDataType, ArrayBuffer[InternalDataType]]()

    table.foreach { tuple =>
      val key = tuple(0)
      val value = tuple(1)
      if (graphBuffer.contains(key)) {
        graphBuffer(key) += value
      } else {
        val buffer = ArrayBuffer(value)
        graphBuffer(key) = buffer
      }
    }

    val graph = mutable.HashMap[InternalDataType, ArraySegment]()

    graphBuffer.foreach {
      case (key, values) =>
        graph(key) = ArraySegment(values.toArray.sorted)
    }

    new GraphTrie(graph)

  }
}

class HashMapTrie(
    rootLevel: ArraySegment,
    nextLevelMap: mutable.HashMap[Int, mutable.HashMap[mutable.ArraySeq[
      InternalDataType
    ], ArraySegment]],
    arity: Int
) extends Trie {

  val tempArrayForIthLevel =
    new Array[mutable.ArraySeq[InternalDataType]](arity)

  init()
  def init() = {
    Range(0, arity).foreach { i =>
      tempArrayForIthLevel(i) = new mutable.ArraySeq[InternalDataType](i + 1)
    }
  }

  override def nextLevel(binding: ArraySegment): ArraySegment = {
    val bindingSize = binding.size
    val level = bindingSize - 1

    if (bindingSize == 0) {
      return rootLevel
    } else {
      val tempArray = tempArrayForIthLevel(level)
      var i = 0
      while (i < bindingSize) {
        tempArray(i) = binding(i)
        i += 1
      }

      return nextLevelMap(level)(tempArray)
    }
  }
  override def toInternalRows(): Array[Array[InternalDataType]] = ???
  override def nextLevel(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = ???

  override def nextLevelWithAdjust(
      binding: ArraySegment,
      inputArraySegment: ArraySegment
  ): Unit = ???

  override def contains(prefix: InternalRow): Boolean = ???

  override def access(): Accessor = ???
}

object HashMapTrie {
  def apply(table: Array[Array[InternalDataType]], arity: Int): HashMapTrie = {
    val rootLevel = ArraySegment(table.map(t => t(0)).distinct.sorted)
    val nextLevelMap =
      mutable.HashMap[Int, mutable.HashMap[mutable.ArraySeq[
        InternalDataType
      ], ArraySegment]]()
    var keyPos = 0
    while (keyPos < arity - 1) {
      val valuePos = keyPos + 1
      val tupleSize = valuePos + 1
      var projectedTable = ArrayBuffer[Array[InternalDataType]]()
      table.foreach { tuple =>
        val projectedTuple = new Array[InternalDataType](tupleSize)
        var j = 0
        while (j <= keyPos + 1) {
          projectedTuple(j) = tuple(j)
          j += 1
        }

        projectedTable += projectedTuple
      }

      projectedTable = projectedTable.distinct

      val levelMap =
        mutable.HashMap[mutable.ArraySeq[InternalDataType], ArrayBuffer[
          InternalDataType
        ]]()

      projectedTable.foreach { tuple =>
        val key = new mutable.ArraySeq[InternalDataType](keyPos + 1)
        var j = 0
        while (j <= keyPos) {
          key(keyPos) = tuple(keyPos)
          j += 1
        }

        levelMap.get(key) match {
          case Some(buffer) => buffer += tuple(valuePos)
          case None =>
            val buffer = ArrayBuffer(tuple(valuePos)); levelMap(key) = buffer
        }
      }

      nextLevelMap(keyPos) =
        levelMap.map(f => (f._1, ArraySegment(f._2.toArray.sorted)))

      keyPos += 1
    }

    new HashMapTrie(rootLevel, nextLevelMap, arity)
  }
}

// Scan the tuples of the relation sequentially, for each tuple,
// locate the bit where it firstly diverge from the previous tuple and then create a new trie node.
object ArrayTrie {
  def apply(table: Array[Array[InternalDataType]], arity: Int): ArrayTrie = {

    //sort the relation in lexical order
    val comparator = new LexicalOrderComparator(arity)
    java.util.Arrays.sort(table, comparator)

    //init
    var idCounter = 0
    var leafIDCounter = -1
    val prevIDs = new Array[Int](arity)
    var prevTuple = new Array[InternalDataType](arity)
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
    val values = new Array[InternalDataType](edges.size - 1)
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

    new ArrayTrie(neighbors, values, neighborsBegin, neighborsEnd, arity)
  }
}

class LexicalOrderComparator(attrNum: Int)
    extends Comparator[Array[InternalDataType]]
    with Serializable {

  override def compare(
      o1: Array[InternalDataType],
      o2: Array[InternalDataType]
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
    extends Comparator[(Int, Int, InternalDataType)]
    with Serializable {

  override def compare(
      o1: (Int, Int, InternalDataType),
      o2: (Int, Int, InternalDataType)
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

case class ValuedEdge(u: Int, v: Int, value: InternalDataType)

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
