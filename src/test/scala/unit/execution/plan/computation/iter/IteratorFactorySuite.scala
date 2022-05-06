//package unit.execution.plan.computation.iter
//
//import org.apache.spark.secco.execution.plan.computation.iter.IteratorFactory
//import org.apache.spark.secco.execution.plan.computation.utils.{ArrayTrie, Trie}
//import util.{SeccoFunSuite, UnitTestTag}
//
////TODO: more testing needed
//class IteratorFactorySuite extends SeccoFunSuite {
//
//  //TODO: test the side effect of init
//
//  test("tableIter", UnitTestTag) {
//    val tuples = Array(
//      Array(0, 1),
//      Array(0, 2),
//      Array(1, 3),
//      Array(2, 4),
//      Array(100, 10000)
//    ).map(_.map(_.toDouble))
//
//    val trie = ArrayTrie(tuples, 2)
//
//    val localAttributeOrder = Array("A", "B")
//
//    val arrayTableIter =
//      IteratorFactory.makeArrayTableIter(tuples, localAttributeOrder)
//    val trieTableIter =
//      IteratorFactory.makeTrieTableIter(trie, localAttributeOrder)
//
//    println(s"arrayTableIter:")
//    pprint.pprintln(arrayTableIter.result())
//
//    println(s"trieTableIter:")
//    pprint.pprintln(trieTableIter.result())
//
//    println(s"trieTableIter.init(Array(0))")
//    //fixme: error could occur if the length of prefix is equal to length of local attribute for PartialLeapfrog.
//    pprint.pprintln(trieTableIter.reset(Array(10, 10)).result())
//
//  }
//
//  test("selectionIter", UnitTestTag) {
//    val tuples = Array(
//      Array(0, 1),
//      Array(0, 2),
//      Array(1, 3),
//      Array(2, 4),
//      Array(100, 10000),
//      Array(1001, 100)
//    ).map(_.map(_.toDouble))
//
//    val trie = ArrayTrie(tuples, 2)
//    val localAttributeOrder = Array("A", "B")
//    val selectionExpr = Seq(("A", "<", "B"))
//
//    val trieTableIter =
//      IteratorFactory.makeTrieTableIter(trie, localAttributeOrder)
//
//    val selectionIter = IteratorFactory.makeSelectIter(
//      trieTableIter,
//      selectionExpr,
//      localAttributeOrder
//    )
//
//    println(s"selectionIter:")
//    pprint.pprintln(selectionIter.result())
//
//    println(s"selectionIter.init(Array(0))")
//    pprint.pprintln(selectionIter.reset(Array(0)).result())
//
//  }
//
//  test("projectIter", UnitTestTag) {
//    val tuples = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(2, 4, 4),
//      Array(100, 10000, 5),
//      Array(1001, 100, 6)
//    ).map(_.map(_.toDouble))
//
//    val trie = ArrayTrie(tuples, 3)
//    val localAttributeOrder = Array("A", "B", "C")
//    val projectionList = Array("A", "C")
//
//    val trieTableIter =
//      IteratorFactory.makeTrieTableIter(trie, localAttributeOrder)
//
//    val projectionIter = IteratorFactory.makeProjectIter(
//      trieTableIter,
//      projectionList,
//      projectionList
//    )
//
//    println(s"projectionIter:")
//    pprint.pprintln(projectionIter.result())
//
//    println(s"projectionIter.init(Array(0))")
//    pprint.pprintln(projectionIter.reset(Array(0)).result())
//
//  }
//
//  test("aggregateIter", UnitTestTag) {
//    val tuples = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(1, 4, 3),
//      Array(2, 4, 4),
//      Array(100, 10000, 5),
//      Array(1001, 100, 6)
//    ).map(_.map(_.toDouble))
//
//    val trie = ArrayTrie(tuples, 3)
//    val localAttributeOrder = Array("A", "B", "C")
//    val groupingList = Array("A", "C")
//    val semiringList = ("count", "*")
//
//    val trieTableIter =
//      IteratorFactory.makeTrieTableIter(trie, localAttributeOrder)
//
//    val aggregateIter = IteratorFactory.makeAggregateIter(
//      trieTableIter,
//      groupingList,
//      semiringList,
//      Array("A", "C", "w")
//    )
//
//    println(s"aggregateIter:")
//    pprint.pprintln(aggregateIter.result())
//
//    println(s"aggregateIter.init(Array(0))")
//    pprint.pprintln(aggregateIter.reset(Array(0, 1)).result())
//
//  }
//
//  test("GHDJoinIter", UnitTestTag) {
//    val tuples1 = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(1, 4, 3),
//      Array(2, 4, 4),
//      Array(100, 10000, 5),
//      Array(1001, 100, 6)
//    ).map(_.map(_.toDouble))
//
//    val tuples2 = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(1, 4, 3),
//      Array(2, 4, 4),
//      Array(100, 10000, 5),
//      Array(1001, 100, 6)
//    ).map(_.map(_.toDouble))
//
//    val schemas = Seq(Seq("A", "B", "C"), Seq("C", "D", "E"))
//
//    val trie1 = ArrayTrie(tuples1, schemas(0).size).asInstanceOf[Trie]
//    val trie2 = ArrayTrie(tuples2, schemas(1).size).asInstanceOf[Trie]
//
//    val tries = trie1 :: trie2 :: Nil toArray
//    val localAttributeOrder = Seq("A", "B", "C", "D", "E").toArray
//
//    val lfIter =
//      IteratorFactory.makeLeapFrogJoinIter(tries, schemas, localAttributeOrder)
//
//    println(s"lfIter:")
//    pprint.pprintln(lfIter.result())
//
//    println(s"lfIter.init(Array(0))")
//    pprint.pprintln(lfIter.reset(Array(0)).result())
//
//  }
//
//  test("FKFKJoinIter", UnitTestTag) {
//    val tuples1 = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(1, 4, 3),
//      Array(2, 4, 4),
//      Array(100, 10000, 5),
//      Array(1001, 100, 6)
//    ).map(_.map(_.toDouble))
//
//    val tuples2 = Array(
//      Array(0, 1, 1),
//      Array(0, 2, 2),
//      Array(1, 3, 3),
//      Array(1, 3, 4),
//      Array(2, 4, 4),
//      Array(100, 5, 10000),
//      Array(1001, 6, 100)
//    ).map(_.map(_.toDouble))
//
//    val trie1 = ArrayTrie(tuples1, 3)
//    val trie2 = ArrayTrie(tuples2, 3)
//    val groupingList = Array("A", "C")
//    val semiringList = ("count", "*")
//
//    val trieTableIter1 =
//      IteratorFactory.makeTrieTableIter(trie1, Array("A", "B", "C"))
//
//    val trieTableIter2 =
//      IteratorFactory.makeTrieTableIter(trie2, Array("A", "C", "D"))
//
//    val aggregateIter = IteratorFactory.makeAggregateIter(
//      trieTableIter2,
//      groupingList,
//      semiringList,
//      Array("A", "C", "w")
//    )
////    pprint.pprintln(aggregateIter.init(Array(0.0, 1.0)).toArray)
//
//    val fkfkJoinIter = IteratorFactory.makeBinaryJoinIter(
//      trieTableIter1,
//      aggregateIter,
//      Array("A", "B", "C", "w")
//    )
//
//    println(s"fkfkJoinIter:")
//    pprint.pprintln(fkfkJoinIter.result())
//
//    println(s"fkfkJoinIter.init(Array(0))")
//    pprint.pprintln(fkfkJoinIter.reset(Array(0)).result())
//
//  }
//
//}
