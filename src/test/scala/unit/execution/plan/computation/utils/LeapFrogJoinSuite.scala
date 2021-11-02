//package unit.execution.plan.computation.utils
//
//import org.apache.spark.secco.execution.plan.computation.utils._
//import org.apache.spark.sql.AnalysisException
//import util.{
//  SeccoFunSuite,
//  ExternalQueryEvaluator,
//  TestDataGenerator,
//  TestQueryGenerator,
//  UnitTestTag
//}
//
//import scala.util.Random
//
//class LeapFrogJoinSuite extends SeccoFunSuite {
//
//  test("trie_iterator", UnitTestTag) {
//    val arity = 2
//    val cardinality = 10
//    val upperBound = 20
//    val tuples1 =
//      TestDataGenerator.genRandomInternalRow(cardinality, arity, upperBound)
//    val trie = Trie(tuples1, arity)
//    pprint.pprintln(trie.access().iterator())
//  }
//
//  test("trie_contains", UnitTestTag) {
//
//    import util.Extension._
//
//    val array = Array(
//      Array(0, 1),
//      Array(1, 2),
//      Array(2, 3)
//    ).toDoubleArray()
//
//    val trie = ArrayTrie(array, 2)
//
//    assert(trie.contains(Array(2, 3)))
//  }
//
//  test("trie", UnitTestTag) {
//
//    for (arity <- 1 until 8) {
//      val cardinality = 1000
//      val upperBound = Int.MaxValue
//      val tuples1 =
//        TestDataGenerator.genRandomInternalRow(cardinality, arity, upperBound)
//      val trie = Trie(tuples1, arity)
//
//      //test if trie.toRelation() is the same as its input
//      val tuples2 = trie.toInternalRows()
//      assert(
//        tuples1.map(_.toSeq).toSeq.diff(tuples2.map(_.toSeq).toSeq).size == 0,
//        "trie.toRelation() is not equal to initial input"
//      )
//
//      //test if trie.nextLevel is correct. we only test the last level of the trie,
//      // which implies other levels function correctly if it functions correctly.
//      if (arity > 1) {
//        Range(0, 100).foreach { _ =>
//          val idx = Random.nextInt(cardinality)
//          val selectedTuple = tuples1(idx)
//          val binding = ArraySegment(selectedTuple.dropRight(1))
//          val value1 = selectedTuple.last
//          val values = trie.nextLevel(binding).toArray()
//          assert(values.contains(value1))
//        }
//      }
//    }
//  }
//
//  //Problem Found:
//  // 1. relation must be set, which means there should be no duplication
//  // 2. error could occur if all relation are unary relation, and their schema is also the same
//  test("leapfrog", UnitTestTag) {
//    val numRelation = 3
//    val arityLimit = 3
//    val cardinality = 10
//    val valueUpperBound = 20
//
//    Range(0, 20).foreach { _ =>
//      try {
//
//        val schemas =
//          TestQueryGenerator
//            .genRandomSchemas(numRelation, arityLimit)
//            .map(_.distinct)
////        val schemas = Seq(Seq("C"), Seq("C"), Seq("C"))
//
//        val relations = Range(0, numRelation)
//          .map(idx =>
//            TestDataGenerator
//              .genRandomInternalRow(
//                cardinality,
//                schemas(idx).size,
//                valueUpperBound
//              )
//              .map(_.toSeq)
//              .distinct
//              .map(_.toArray)
//          )
//          .toArray
////
////        val relations = Array(
////          Array(
////            Array(3.0),
////            Array(9.0),
////            Array(18.0),
////            Array(12.0),
////            Array(4.0),
////            Array(1.0),
////            Array(11.0),
////            Array(19.0)
////          ),
////          Array(
////            Array(15.0),
////            Array(0.0),
////            Array(19.0),
////            Array(18.0),
////            Array(17.0),
////            Array(14.0),
////            Array(13.0),
////            Array(16.0)
////          ),
////          Array(
////            Array(18.0),
////            Array(10.0),
////            Array(14.0),
////            Array(11.0),
////            Array(17.0),
////            Array(0.0)
////          )
////        )
//
//        val queryResultSize =
//          ExternalQueryEvaluator.validateNaturalJoinQuery(relations, schemas)
//
//        //test leapfrog join using randomly generated natural join query
//        val attributeOrder = Random.shuffle(schemas.flatten.distinct)
//
//        //DEBUG
////        pprint.pprintln(attributeOrder)
////        pprint.pprintln(schemas)
////        pprint.pprintln(relations)
//
//        relations.zip(schemas).foreach {
//          case (tuples, schema) =>
//            Alg.reorder(attributeOrder, schema, tuples)
//        }
//        val tries = relations.zip(schemas).map {
//          case (tuples, schema) =>
//            ArrayTrie(tuples, schema.size).asInstanceOf[Trie]
//        }
//
//        val lf = LeapFrogJoin(tries, attributeOrder, schemas)
//        val lfResult = lf.map(_.clone()).toArray
//
//        //DEBUG
////        pprint.pprintln(lfResult)
////        pprint.pprintln(queryResultSize)
////        pprint.pprintln(lfResult.size)
//
//        assert(queryResultSize == lfResult.size)
//      } catch {
//        case e: AnalysisException =>
//      }
//    }
//  }
//
//  test("partialLeapFrog", UnitTestTag) {
//
//    val numRelation = 3
//    val arityLimit = 3
//    val cardinality = 10
//    val valueUpperBound = 20
//
//    Range(0, 100).foreach { _ =>
//      try {
//
//        val schemas =
//          TestQueryGenerator
//            .genRandomSchemas(numRelation, arityLimit)
//            .map(_.distinct)
//
//        val relations = Range(0, numRelation)
//          .map(idx =>
//            TestDataGenerator
//              .genRandomInternalRow(
//                cardinality,
//                schemas(idx).size,
//                valueUpperBound
//              )
//              .map(_.toSeq)
//              .distinct
//              .map(_.toArray)
//          )
//          .toArray
//
//        //test leapfrog join using randomly generated natural join query
//        val attributeOrder = Random.shuffle(schemas.flatten.distinct)
//
//        //DEBUG
////        pprint.pprintln(attributeOrder)
////        pprint.pprintln(schemas)
////        pprint.pprintln(relations)
//
//        relations.zip(schemas).foreach {
//          case (tuples, schema) =>
//            Alg.reorder(attributeOrder, schema, tuples)
//        }
//        val tries = relations.zip(schemas).map {
//          case (tuples, schema) =>
//            ArrayTrie(tuples, schema.size).asInstanceOf[Trie]
//        }
//
//        val lf = LeapFrogJoin(tries, attributeOrder, schemas)
//        val plf = PartialLeapFrogJoin(tries, attributeOrder, schemas)
//        val lfResult = lf.map(_.clone()).toArray
//
//        //DEBUG
////        pprint.pprintln(s"lfResult")
////        pprint.pprintln(lfResult)
//
//        if (lfResult.nonEmpty) {
//          val head = lfResult.head
//          if (lfResult.head.size > 1) {
//            val key = head(0)
//            val values = lfResult.filter(f => f(0) == key)
//            val valuesOfPlf = plf.unsafeInit(Array(key)).map(_.clone()).toArray
//
//            //DEBUG
////            pprint.pprintln(s"values")
////            pprint.pprintln(values)
////            pprint.pprintln(s"valuesOfPlf")
////            pprint.pprintln(valuesOfPlf)
//
//            assert(values.map(_.toSeq).diff(valuesOfPlf.map(_.toSeq)).isEmpty)
//          }
//        }
//
//      } catch {
//        case e: AnalysisException =>
//      }
//    }
//
//  }
//
//}
