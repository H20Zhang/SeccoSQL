package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.catalog.AbstractCatalogTable
import org.apache.spark.secco.expression.{Attribute, AttributeSeq}

import scala.collection.mutable.ArrayBuffer

/** A decomposer for decomposing a multiway natural join into [[RelationGHDTree]] */
object GHDDecomposer {

  /** Decompose a multiway natural join, which represented as Seq[Seq[Attributes]], into [[RelationGHDTree]].
    * @param schemas the schema that represents the multiway natural join.
    * @return all possible [[RelationGHDTree]] given the multiway natural join.
    */
  def decomposeTree(
      schemas: Seq[Seq[Attribute]]
  ): IndexedSeq[RelationGHDTree] = {

    //filter the schemas that are contained inside another schema
    val excludedSchemas = schemas.filter { s1 =>
      schemas
        .diff(Seq(s1))
        .exists(s2 => s1.attrs.diff(s2.attrs).isEmpty)
    }
    val schemasForGHD = schemas.diff(excludedSchemas)

    //find the GHD Decomposition for the schemasForGHD
    val graph = RelationGraph(schemasForGHD.map(_.toArray).toArray)
    val ghds = HyperTreeDecomposer.genAllGHDs(graph)

    //construct RelationGHD
    ghds
      .map { t =>
        val edgeToSchema =
          schemasForGHD.map(f => (f.toSet, f)).toMap
        val bags =
          t.nodes
            .map(f => (f.id, f.g.edges.map(edge => edgeToSchema(edge.attrs))))
            .map { case (idx, bag) =>
              //add previously filtered schemas to the bags that contained it.
              val fullBag = bag ++ bag
                .flatMap(schema1 =>
                  excludedSchemas.filter(schema2 =>
                    schema2
                      .diff(schema1)
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
                .findNode(f._1)
                .flatMap(f => f)
                .distinct
                .intersect(
                  relationGHD
                    .findNode(f._2)
                    .flatMap(f => f)
                    .distinct
                )
                .size
            )
            .sum
        )
      )
  }
}

/** A GHDTree that contains hypernodes that represents joins between relations, and hyperedge that represents joins between hypernodes
  * @param nodes hypernodes of the GHDTree
  * @param edges hyperedges of the GHDTree
  * @param fhtw width of the GHDTree
  */
case class RelationGHDTree(
    nodes: Seq[(Int, Seq[Seq[Attribute]])],
    edges: Seq[(Int, Int)],
    fhtw: Double
) {

  val findNode = nodes.toMap
  val schemas = nodes.flatMap(_._2).distinct

  def getSchemas(nodeId: Int) = {
    findNode(nodeId)
  }

  override def toString: String = {
    s"""
       |node:${nodes
      .map(f =>
        (
          f._1,
          f._2.map(g => g.mkString("(", ",", ")")).mkString(",")
        )
      )
      .mkString("(", ",", ")")}
       |edge:${edges.mkString("(", ",", ")")}
       |fhtw:${fhtw}
       |""".stripMargin
  }
}
