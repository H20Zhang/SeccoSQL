//package org.apache.spark.secco.benchmark.testcases
//
//import org.apache.spark.secco.Dataset
//import org.apache.spark.secco.benchmark.{SubgraphBenchmark}
//
///**
//  * Base class of benchmark related to basic subgraph matching query (just enumerating the matched patterns, and return
//  * numbers of matched results).
//  */
//abstract class CountSubgraphBenchmark extends SubgraphBenchmark {
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val ds = super.genQuery(inputData)
//    ds.aggregate("count(*) by a")
//  }
//}
//
///* == Debug Query == */
//
//object Debug1 extends SubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-a"
//}
//
///* == Subgraph Queries == */
//
//object S1 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-a"
//}
//
//object S2 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-a;c-d"
//}
//
//object S3 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-a;b-d"
//}
//
//object S4 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-a;a-c;b-d"
//}
//
//object S5 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-e"
//}
//
//object S6 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-e;c-e"
//}
//
//object S7 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;a-c;b-d;c-e"
//}
//
//object S8 extends CountSubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-d;b-e;c-e"
//}
//
//object C1 extends SubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-e"
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val ds = super.genQuery(inputData)
//    ds.project("b, d").aggregate("count(*) by b")
//  }
//}
//
//object C2 extends SubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-e;c-e"
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val ds = super.genQuery(inputData)
//    ds.project("b, c").aggregate("count(*) by b")
//  }
//}
//
//object C3 extends SubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String = "a-b;b-c;c-d;d-e;a-e;b-e;d-w"
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val ds = super.genQuery(inputData)
//    ds.aggregate("sum(w) by a")
//  }
//}
//
//object C4 extends SubgraphBenchmark {
//  override def edgesOfSubgraphQuery: String =
//    "a-b;b-c;c-d;d-e;a-e;b-e;c-e;d-w"
//
//  override protected def genQuery(inputData: Map[String, Dataset]): Dataset = {
//    val ds = super.genQuery(inputData)
//    ds.aggregate("sum(w) by a")
//  }
//}
