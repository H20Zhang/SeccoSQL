package org.apache.spark

package object secco {
  object PreferenceLevel extends Enumeration {
    type PreferenceLevel = Value
    val SatisfyAll, SatisfyOne, MaybeSatisfy = Value
  }

  def debug(x: Any): Unit = {
    pprint.pprintln(s"[debug]:${x}")
  }
}

//type RawGraph = DefaultUndirectedGraph[Int, DefaultEdge]
//type Mapping = Map[NodeID, NodeID]
//
//// normal graph
//type NodeID = Int
//type Edge = (NodeID, NodeID)
//type NodeList = Seq[NodeID]
//type EdgeList = Seq[Edge]
//type ADJList = mutable.HashMap[NodeID, ArrayBuffer[NodeID]]
//
//// hypertree, where a hypernode is a graph
//type GraphID = Int
//
//trait TestAble {
//  def test(): Unit
//}
//
//trait IsomorphicAble {
//  def isIsomorphic(p: IsomorphicAble): Boolean
//
//  def findIsomorphism(p: IsomorphicAble): Seq[Mapping]
//
//  def findIsomorphismUnderConstriant(
//                                      p: IsomorphicAble,
//                                      constraint: Mapping
//                                    ): Seq[Mapping]
//
//  def findAutomorphism(): Seq[Mapping]
//
//  def findAutomorphismUnderConstriant(constraint: Mapping): Seq[Mapping]
//}
//
//trait LogAble {
//  org.apache.log4j.PropertyConfigurator
//    .configure("./src/resources/log4j.preperties")
//
//  lazy val logger: Logger = Logger.getLogger(this.getClass)
//}
//
//
//
//object Stage extends Enumeration {
//  type Stage = Value
//  val Unprocessed, SymmetryBreaked, NonInduceInstanceRemoved,
//  NonIsomorphismRemoved, Decomposed, Optimized = Value
//}
//
//object CountAttrCounter {
//  var count = 0
//  def nextCountAttrId() = {
//    val old = count
//    count += 1
//    val countAttr = s"Count${old}"
//    val catalog = Catalog.defaultCatalog()
//
//    val id = catalog.registerAttr(countAttr)
//
//    id
//  }
//}
