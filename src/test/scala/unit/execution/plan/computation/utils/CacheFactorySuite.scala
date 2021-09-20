package unit.execution.plan.computation.utils

import org.apache.spark.secco.execution.plan.computation.utils.CacheFactory
import util.{SeccoFunSuite, UnitTestTag}

class CacheFactorySuite extends SeccoFunSuite {

  test("LRUCache", UnitTestTag) {
    val lruCahce = CacheFactory.genLRUCache(Array("A", "B"), Array("C", "D"))
    val row1 = Array(0, 1, 2, 3).map(_.toDouble)
    val row2 = Array(1, 2, 3, 4).map(_.toDouble)
    val row3 = Array(3, 4, 5, 5).map(_.toDouble)

    val it = Seq(row1, row2, row3).toIterator

    assert(lruCahce.contains(Array(0, 1)) == false)

    lruCahce.put(Array(0, 1), Seq(row1).toIterator)
    lruCahce.put(Array(1, 2), Seq(row2).toIterator)
    lruCahce.put(Array(3, 4), Seq(row3).toIterator)

    row1(3) = 1000
    row2(3) = 1000
    row3(3) = 1000

    assert(
      lruCahce.get(Array(0, 1)).toSeq.map(_.toSeq) == Seq(Seq(0, 1, 2, 3))
    )

    assert(
      lruCahce.get(Array(1, 2)).toSeq.map(_.toSeq) == Seq(Seq(1, 2, 3, 4))
    )

    assert(
      lruCahce.get(Array(2, 3)).hasNext == false
    )

  }

  test("LastUsedCache", UnitTestTag) {

    val lastUsedCache =
      CacheFactory.genLastUsedCache(Array("A", "B"), Array("C", "D"))
    val row1 = Array(0, 1, 2, 3).map(_.toDouble)
    val row2 = Array(1, 2, 3, 4).map(_.toDouble)
    val row3 = Array(2, 3, 5, 5).map(_.toDouble)

    val it = Seq(row1, row2, row3).toIterator

    assert(lastUsedCache.contains(Array(0, 1)) == false)

    lastUsedCache.put(Array(0, 1), Seq(row1).toIterator)
    lastUsedCache.put(Array(1, 2), Seq(row2).toIterator)
    lastUsedCache.put(Array(2, 3), Seq(row3).toIterator)

    row1(3) = 1000
    row2(3) = 1000
    row3(3) = 1000

    assert(
      lastUsedCache.contains(Array(0, 1)) == false
    )

    assert(
      lastUsedCache.contains(Array(1, 2)) == false
    )

    assert(
      lastUsedCache.get(Array(2, 3)).toList.map(_.toSeq) == Seq(Seq(2, 3, 5, 5))
    )
  }

  test("LastUsedSetCache", UnitTestTag) {

    val lastUsedCache =
      CacheFactory.genLastUsedCache(Array("A", "B"), Array())
    val row1 = Array(0, 1).map(_.toDouble)
    val row2 = Array(1, 2).map(_.toDouble)
    val row3 = Array(2, 3).map(_.toDouble)

    val it = Seq(row1, row2, row3).toIterator

    assert(lastUsedCache.contains(Array(0, 1)) == false)

    lastUsedCache.put(Array(0, 1), Seq(row1).toIterator)
    lastUsedCache.put(Array(1, 2), Seq(row2).toIterator)
    lastUsedCache.put(Array(2, 3), Seq(row3).toIterator)

    row1(1) = 1000
    row2(1) = 1000
    row3(1) = 1000

    assert(
      lastUsedCache.contains(Array(0, 1)) == false
    )

    assert(
      lastUsedCache.contains(Array(1, 2)) == false
    )

    assert(
      lastUsedCache.get(Array(2, 3)).toList.map(_.toSeq) == Seq(Seq(2, 3))
    )
  }

  test("LRUSetCache", UnitTestTag) {
    val lruCahce = CacheFactory.genLRUCache(Array("A", "B"), Array())
    val row1 = Array(0, 1).map(_.toDouble)
    val row2 = Array(1, 2).map(_.toDouble)
    val row3 = Array(3, 4).map(_.toDouble)

    assert(lruCahce.contains(Array(0, 1)) == false)

    lruCahce.put(Array(0, 1), Seq(row1).toIterator)
    lruCahce.put(Array(1, 2), Seq(row2).toIterator)
    lruCahce.put(Array(3, 4), Seq(row3).toIterator)

    row1(0) = 1000
    row2(1) = 1000
    row3(0) = 1000

    assert(
      lruCahce.get(Array(0, 1)).toSeq.map(_.toSeq) == Seq(Seq(0, 1))
    )

    assert(
      lruCahce.get(Array(1, 2)).toSeq.map(_.toSeq) == Seq(Seq(1, 2))
    )

    assert(
      lruCahce.get(Array(2, 3)).hasNext == false
    )

  }

}
