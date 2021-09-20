package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.catalog.AbstractCatalogTable

import scala.collection.mutable.ArrayBuffer

class GHDDecomposer {
  def decomposeTree(
      schemas: Seq[AbstractCatalogTable]
  ): IndexedSeq[RelationGHDTree] = {

    //filter the schemas that are contained inside another schema
    val excludedSchemas = schemas.filter { s1 =>
      schemas
        .diff(Seq(s1))
        .exists(s2 => s1.attributeNames.diff(s2.attributeNames).isEmpty)
    }
    val schemasForGHD = schemas.diff(excludedSchemas)

    //find the GHD Decomposition for the schemasForGHD
    val graph = RelationGraph(schemasForGHD.toArray)
    val ghds = HyperTreeDecomposer.genAllGHDs(graph)

    //construct RelationGHD
    ghds
      .map { t =>
        val edgeToSchema =
          schemasForGHD.map(f => (f.attributeNames.toSet, f)).toMap
        val bags =
          t.nodes
            .map(f => (f.id, f.g.edges.map(edge => edgeToSchema(edge.attrs))))
            .map {
              case (idx, bag) =>
                //add previously filtered schemas to the bags that contained it.
                val fullBag = bag ++ bag
                  .flatMap(schema1 =>
                    excludedSchemas.filter(schema2 =>
                      schema2.attributeNames
                        .diff(schema1.attributeNames)
                        .isEmpty
                    )
                  )
                  .distinct

                (idx, fullBag)
            }
        val connections = t.edges.map(e => (e.src.id, e.dst.id))

        RelationGHDTree(
          bags.toSeq.map(f => (f._1, f._2.toSeq)),
          connections.toSeq,
          t.fractionalHyperNodeWidth
        )
      }
      .sortBy(relationGHD =>
        (
          relationGHD.fhtw,
          -relationGHD.edges.size,
          relationGHD.edges
            .map(f =>
              relationGHD
                .idToGHDNode(f._1)
                .flatMap(f => f.attributeNames)
                .distinct
                .intersect(
                  relationGHD
                    .idToGHDNode(f._2)
                    .flatMap(f => f.attributeNames)
                    .distinct
                )
                .size
            )
            .sum
        )
      )
  }
}

object GHDDecomposer {
  lazy val defaultDecomposer = new GHDDecomposer
}

case class RelationGHDTree(
    nodes: Seq[(Int, Seq[AbstractCatalogTable])],
    edges: Seq[(Int, Int)],
    fhtw: Double
) {

  val idToGHDNode = nodes.toMap
  val schemas = nodes.flatMap(_._2).distinct

  def getSchemas(nodeId: Int) = {
    idToGHDNode(nodeId)
  }

  //enumerate all the possible traversal orders of the ghd
  def allTraversalOrder: Seq[Seq[Int]] = {

    val fullDirE = edges.flatMap(f => Seq(f, f.swap))
    val ids = nodes.map(_._1)

    //all traversalOrders
    var traversalOrders = ids.permutations.toSeq

    //only retain connected traversalOrders
    traversalOrders = traversalOrders.filter { order =>
      var valid = true
      var i = 1
      while (i < order.size) {
        val subPath = order.slice(0, i)
        if (!subPath.exists(j => fullDirE.contains(j, order(i)))) {
          valid = false
        }
        i += 1
      }
      valid
    }

    traversalOrders
  }

  //  find the compatible attribute orders for an given traversal order
  //  TODO: the meaning of function below is unknown,
  //    might be a special class for determining leapfrog aggregate order
  def compatibleAttrOrder(traversalOrder: Seq[Int]): Seq[Array[String]] = {

    if (traversalOrder.isEmpty) {
      return Seq()
    }

    val firstAttrs =
      idToGHDNode(traversalOrder.head).flatMap(_.attributeNames).distinct.toSeq
    var attrOrders = firstAttrs.permutations.toSeq

    var remainingTraversalOrder = traversalOrder.drop(1)
    val assignedAttrIds = ArrayBuffer[String]()
    assignedAttrIds ++= firstAttrs

    while (remainingTraversalOrder.nonEmpty) {
      val nextAttrIds = idToGHDNode(remainingTraversalOrder.head)
        .flatMap(_.attributeNames)
        .distinct
        .diff(assignedAttrIds)
      val nextAttrOrders = nextAttrIds.permutations.toSeq
      attrOrders = attrOrders.flatMap { attrOrder =>
        nextAttrOrders.map(nextOrder => attrOrder ++ nextOrder)
      }

      remainingTraversalOrder = remainingTraversalOrder.drop(1)
      assignedAttrIds ++= nextAttrIds
    }

    attrOrders.map(_.toArray)
  }

  override def toString: String = {
    s"""
       |node:${nodes
      .map(f =>
        (
          f._1,
          f._2.map(g => g.attributeNames.mkString("(", ",", ")")).mkString(",")
        )
      )
      .mkString("(", ",", ")")}
       |edge:${edges.mkString("(", ",", ")")}
       |fhtw:${fhtw}
       |""".stripMargin
  }
}

//case class RelationGHDStar(
//    core: Seq[Schema],
//    leaves: Seq[Seq[Schema]],
//    fhsw: Double
//) {
//
//  val coreAttrIds = core.flatMap(_.attributes).distinct
//
//  def isSingleAttrLeafStar(): Boolean = {
//    leaves.forall { schemas =>
//      val leafAttrIds = schemas.flatMap(_.attributes).distinct
//      leafAttrIds.diff(coreAttrIds).size == 1
//    }
//  }
//
//  def factorizeSingleAttrOrder(): (Seq[String], Int) = {
//    if (isSingleAttrLeafStar()) {
//      val leaveIds = leaves.map { schemas =>
//        val leafAttrIds = schemas.flatMap(_.attributes).distinct
//        leafAttrIds.diff(coreAttrIds)(0)
//      }
//
//      (coreAttrIds ++ leaveIds, coreAttrIds.size - 1)
//    } else {
//      throw new Exception("Star does not exists factorizeSingleAttrOrder")
//    }
//  }
//
//  override def toString: String = {
//    s"""
//       |core:${core}
//       |
//       |leaves:${leaves}
//       |
//       |fhsw:${fhsw}
//       |""".stripMargin
//  }
//
//}

//def decomposeStar(
//isSingleAttrFactorization: Boolean = true
//): IndexedSeq[RelationGHDStar] = {
//  //filter the schemas that are contained inside another schema
//  val containedSchemas = schemas.filter { s1 =>
//  schemas
//  .diff(Seq(s1))
//  .exists(s2 => s1.attributes.diff(s2.attributes).isEmpty)
//}
//  val notContainedSchemas = schemas.diff(containedSchemas)
//
//  //find the GHD Decomposition for the notContainedSchemas
//  val E =
//  notContainedSchemas.map(f => RelationEdge(f.attributes.toSet)).toArray
//  val V = E.flatMap(_.attrs).distinct.toArray
//  val graph = RelationGraph(V, E)
//  val ghds = HyperTreeDecomposer.genAllGHDs(graph)
//
//  //filter out the HyperStar and construct RelationGHD
//  val stars = ghds.par
//  .filter { t =>
//  val adjList = t.edges
//  .map(edge => (edge.u.id, edge.v.id))
//  .flatMap(edge => Iterator(edge, edge.swap))
//  .groupBy(_._1)
//  .map(g => (g._1, g._2.map(_._2)))
//  .toMap
//
//  val numOfNodes = t.nodes.size
//
//  if (numOfNodes == 1) {
//  false
//} else {
//  t.nodes.exists { n =>
//  adjList(n.id).size == (numOfNodes - 1)
//}
//}
//}
//  .map { t =>
//  val edgeToSchema =
//  notContainedSchemas.map(f => (f.attributes.toSet, f)).toMap
//  val bagMaps =
//  t.nodes
//  .map(f => (f.id, f.g.E().map(edge => edgeToSchema(edge.attrs))))
//  .map {
//  case (idx, bag) =>
//  //add previously filtered schemas to the bagMaps that contained it.
//  val fullBag = bag ++ bag
//  .flatMap(schema1 =>
//  containedSchemas.filter(schema2 =>
//  schema2.attributes.diff(schema1.attributes).isEmpty
//  )
//  )
//  .distinct
//
//  (idx, fullBag)
//}
//  .toMap
//
//  val adjList = t.edges
//  .map(edge => (edge.u.id, edge.v.id))
//  .flatMap(edge => Iterator(edge, edge.swap))
//  .groupBy(_._1)
//  .map(g => (g._1, g._2.map(_._2)))
//  .toMap
//
//  val numOfNodes = t.nodes.size
//  val rootId = t.nodes
//  .filter { n =>
//  adjList(n.id).size == (numOfNodes - 1)
//}
//  .head
//  .id
//  val root = bagMaps(rootId)
//  val leaves = bagMaps.keys.toSeq.diff(Seq(rootId)).map(bagMaps)
//
//  RelationGHDStar(
//  root,
//  leaves.map(_.toSeq),
//  t.fractionHyperStarWidth(rootId)
//  )
//}
//  .toArray
//
//  if (isSingleAttrFactorization) {
//  stars
//  .filter(_.isSingleAttrLeafStar())
//  .sortBy(relationGHDStar =>
//  (relationGHDStar.fhsw, -relationGHDStar.leaves.size)
//  )
//} else {
//  stars.sortBy(relationGHDStar =>
//  (relationGHDStar.fhsw, -relationGHDStar.leaves.size)
//  )
//}
//
//}
