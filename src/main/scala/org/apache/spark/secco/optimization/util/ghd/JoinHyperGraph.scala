package org.apache.spark.secco.optimization.util.ghd

import org.apache.spark.secco.catalog.AbstractCatalogTable
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.expression.{
  And,
  Attribute,
  AttributeReference,
  AttributeSeq,
  EqualTo,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.optimization.plan.{
  CyclicJoinProperty,
  EquiJoinProperty,
  MultiwayJoin,
  Project
}
import org.apache.spark.secco.optimization.util.joingraph.JoinGraph
import org.apache.spark.secco.optimization.util.{Edge, Graph, HasID, Node}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/** A decomposer for decomposing a multiway natural join into [[RelationGHDTree]] */
@deprecated
object RelationGHDTreeDecomposer {

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
    val graph = GHDHyperGraph(schemasForGHD.map(_.toArray).toArray)
    val ghds = GHDDecomposer.genAllGHDs(graph)

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
@deprecated
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

/** A node in the [[JoinHyperGraph]], which actually represents an attribute.
  *
  * @param id id of the node
  * @param attr attributes of the node
  */
case class JoinHyperGraphNode(id: Int, attr: Attribute) extends Node {
  override def verboseString: String = attr.toString
}

object JoinHyperGraphNode {

  private var counter = 0
  private val attrToId = scala.collection.mutable.HashMap[Attribute, Int]()

  def apply(attr: Attribute): JoinHyperGraphNode = {
    attrToId.get(attr) match {
      case Some(id) => JoinHyperGraphNode(id, attr)
      case None =>
        counter += 1; attrToId(attr) = counter;
        JoinHyperGraphNode(counter, attr)
    }
  }
}

/** A edge in the [[JoinHyperGraph]], which actually represents a relation.
  *
  * @param attrs schemas of the relation.
  */
case class JoinHyperGraphEdge(attrs: Set[Attribute], relation: LogicalPlan)
    extends Edge[JoinHyperGraphNode] {
  override val nodes: Array[JoinHyperGraphNode] =
    attrs.map(attr => JoinHyperGraphNode(attr)).toArray

  override def verboseString: String = {
    attrs.mkString("(", ",", ")") + " -> " + relation.simpleString
  }
}

/** A hypergraph that represents a join query, where each node is an attribute and each edge is an relation.
  *
  * Note that: As hypergraph for join query is defined for Natural Join Query, for Equi-Join Query, it needs to be
  * "converted" into Natural Join Query. To do this, we collect attributes that are equivalent in [[EqualTo]] condition
  * into equivalent attributes set. We then replace attributes by their representative attribute whenever possible in
  * this class.
  *
  * <pre>
  * For example:
  *
  *       [original equi-join query]                    [natural join query]        [representative attributes]
  * BinaryJoin(R1(A, B), R2(B, C), "R1.A=R2.B") => NaturalJoin(R1(A, B), R2(B, C))      (B, (R1.B, R2.B))
  * </pre>
  * @param nodes nodes of the graph
  * @param edges edges of the graph
  * @param attr2RepAttr the mapping from attributes in [[LogicalPlan]] to representative attributes
  */
case class JoinHyperGraph(
    override val nodes: Array[JoinHyperGraphNode],
    override val edges: Array[JoinHyperGraphEdge],
    attr2RepAttr: AttributeMap[Attribute]
) extends Graph(nodes, edges) {

  /** The map from attrs of edge to edge.
    * Note that multiple edges could have same attrs
    */
  private lazy val schema2Edges =
    edges.map(edge => (edge.attrs, edge)).groupBy(_._1)

  /** All [[GHDHyperTree]] can be obtained from this hypergraph. */
  private lazy val allGHDs: Seq[GHDHyperTree] = {

    val ghdHyperGraph = GHDHyperGraph(schema2Edges.keys.toArray.map(_.toArray))

//    println(s"[debug] ghd:${ghdHyperGraph}")

    GHDDecomposer.genAllGHDs(
      ghdHyperGraph
    )
  }

  // find join conditions of hypergraph attributes.
  // the input is the hyperedges of the hypergraph, the output is the equi-join condition.
  private def constructJoinConditions(
      l: Seq[Attribute],
      lPlan: LogicalPlan,
      r: Seq[Attribute],
      rPlan: LogicalPlan
  ): Option[Expression] = {

    l.intersect(r)
      .map { joinAttr =>
        val attrInLPlan = attr2RepAttr
          .find { case (attr, representativeAttr) =>
            lPlan.outputSet.contains(attr) && representativeAttr == joinAttr
          }
          .get
          ._1

        val attrInRPlan = attr2RepAttr
          .find { case (attr, representativeAttr) =>
            rPlan.outputSet.contains(attr) && representativeAttr == joinAttr
          }
          .get
          ._1

        EqualTo(attrInLPlan, attrInRPlan)
      }
      .reduceOption(And)
  }

  private def constructMultiwayJoin(
      hyperTreeNode: GHDHyperTreeNode,
      usedRelations: mutable.HashSet[LogicalPlan]
  ): MultiwayJoin = {

    val attrMap = mutable.HashMap[Attribute, Attribute]()

    // Input relations to multiway join are all corresponding relations of edges of JoinHyperGraph of HyperTreeNode.
    // Note that we need to handle the duplicate relation cases.
    val inputs = hyperTreeNode.g.edges
      .flatMap(edge => schema2Edges(edge.attrs).map(_._2.relation))
      .map { plan =>
        if (usedRelations.contains(plan)) {

          // Generate new attributes for duplicated input.
          val newAttributes = plan.output.map(_.newInstance())

          // Record the mapping between old attributes and new attributes.
          attrMap ++= plan.output.zip(newAttributes)

          Project(plan, newAttributes)
        } else {
          usedRelations += plan
          plan
        }
      }

    // Find conditions for multiway join.
    val conditions =
      hyperTreeNode.g.edges
        .combinations(2)
        .flatMap { case Array(l, r) =>
          val lInputs = schema2Edges(l.attrs).map(_._2)
          val rInputs = schema2Edges(r.attrs).map(_._2)

          val conditionBuf = ArrayBuffer[Expression]()

          for (lInput <- lInputs) {
            for (rInput <- rInputs) {
              constructJoinConditions(
                lInput.attrs.toSeq,
                lInput.relation,
                rInput.attrs.toSeq,
                rInput.relation
              ).foreach { expr =>
                // Replace new attributes to old attributes.
                val newExpr = expr transform {
                  case a: Attribute if attrMap.contains(a) => attrMap(a)
                }

                conditionBuf += newExpr
              }
            }
          }

          conditionBuf
        }
        .toSeq

    if (hyperTreeNode.g.width > 1.0) {
      MultiwayJoin(
        inputs,
        conditions,
        Set(CyclicJoinProperty, EquiJoinProperty)
      )
    } else {
      MultiwayJoin(inputs, conditions, Set(EquiJoinProperty))
    }

  }

  /** Construct a GHD-based join plan from the given [[GHDHyperTree]]. */
  private def constructGHDBasedJoinPlan(
      ghdHyperTree: GHDHyperTree
  ): LogicalPlan = {

    val usedRelations = mutable.HashSet[LogicalPlan]()

    // Convert all hypertreeNodes to MultiwayJoin.
    val hypertreeNode2MultiwayJoin =
      ghdHyperTree.nodes
        .map(node => (node, constructMultiwayJoin(node, usedRelations)))
        .toMap

    // Find all join conditions between MultiwayJoins (HyperTreeNodes).
    val conditions = ghdHyperTree.edges.flatMap { edge =>
      val lOutput = edge.src.g.nodes.map(_.attr)
      val rOutput = edge.dst.g.nodes.map(_.attr)

      constructJoinConditions(
        lOutput,
        hypertreeNode2MultiwayJoin(edge.src),
        rOutput,
        hypertreeNode2MultiwayJoin(edge.dst)
      )
    }

    val joinGraph =
      JoinGraph(hypertreeNode2MultiwayJoin.values.toSeq, conditions)

//    pprint.pprintln(s"[debug]:joinGraph${joinGraph}")

    joinGraph.toPlan() transform {
      case m: MultiwayJoin if m.children.size == 1 => m.children.head
    }
  }

  /** Return true if this hypergraph is an cyclic hypergraph. */
  def isCyclic(): Boolean = !isAcyclic()

  def isAcyclic(): Boolean = allGHDs.exists(_.fractionalHyperNodeWidth == 1.0)

  /** Generate an [[LogicalPlan]] corresponds to the optimal [[GHDHyperTree]] in terms of given `rankFunction`.
    * @param filterFunction the function to filter unpromising [[GHDHyperTree]]
    * @param rankFunction the function to give each [[GHDHyperTree]] a weight for ranking
    * @return new optimized [[LogicalPlan]]
    */
  def ghdPlan(
      filterFunction: GHDHyperTree => Boolean,
      rankFunction: (GHDHyperTree, GHDHyperTree) => Boolean = {
        import scala.math.Ordering.Implicits._
        (lTree, rTree) =>
          (
            lTree.fractionalHyperNodeWidth,
            lTree.nodes.size
          ) < (rTree.fractionalHyperNodeWidth, rTree.nodes.size)
      }
  ): LogicalPlan = {
    val optimalGHDOpt = allGHDs
      .filter(filterFunction)
      .sortWith(rankFunction)
      .headOption

    optimalGHDOpt
      .map { case ghd =>
        constructGHDBasedJoinPlan(ghd)
      }
      .getOrElse(
        throw new Exception(s"no GHD found for JoinHyperGraph:${this}")
      )
  }

  /** Return this hypergraph as a [[MultiwayJoin]]. */
  def toPlan(): LogicalPlan = ghdPlan(_ => true)

  override def toString: String = {
    super.toString + s"\nRepresentative Attributes:${attr2RepAttr.toSeq
      .map(_.swap)
      .groupBy(_._1)
      .map(f => (f._1, f._2.map(_._2)))}"
  }
}

object JoinHyperGraph extends PredicateHelper {

  def apply(
      children: Seq[LogicalPlan],
      condition: Seq[Expression]
  ): JoinHyperGraph = {

    // Construct the set of equivalence attributes in terms of EqualTo.
    val equivSet = {

      // Find out all equivalence relationship.
      val equiv = condition.flatMap(splitConjunctivePredicates).map { f =>
        f match {
          case EqualTo(a: AttributeReference, b: AttributeReference) =>
            if (
              children.exists(plan =>
                plan.outputSet.contains(a) && plan.outputSet.contains(b)
              )
            ) {
              throw new Exception(
                s"join graph only allow join condition between relation, invalid condition:${f.sql}"
              )
            } else {
              (a, b)
            }
          case _ =>
            throw new Exception(
              s"only equi-join is allowed in multiway join's condition, found invalid condition ${f}"
            )
        }
      }

      //propagate the equivalence relationship
      val equivSetArr = ArrayBuffer[AttributeSet]()
      equiv.foreach { case (a, b) =>
        var newEquivSet = AttributeSet(a :: b :: Nil)
        var oldEquivSetOpt: Option[AttributeSet] = None

        //check if a or b are already contains in the equivSet. If so expand equivSet by adding b or a.
        equivSetArr.foreach { equivSet =>
          if (equivSet.contains(a)) { // add to existing equivSet
            oldEquivSetOpt = Some(equivSet)
            newEquivSet = equivSet ++ AttributeSet(b)
          } else if (equivSet.contains(b)) { // add to existing equivSet
            oldEquivSetOpt = Some(equivSet)
            newEquivSet = equivSet ++ AttributeSet(a)
          }
        }

        // remove old equivSet
        oldEquivSetOpt.foreach(oldEquivSet =>
          equivSetArr.remove(equivSetArr.indexOf(oldEquivSet))
        )

        // add new equivSet
        equivSetArr += newEquivSet
      }

      equivSetArr
    }

    // give each equivSet a representative element
    val representativeAttributes = equivSet.map(_.head.newInstance())

    val joinAttr2RepresentativeAttr =
      AttributeMap(representativeAttributes.zip(equivSet).flatMap {
        case (representativeAttr, equivSet) =>
          equivSet.toSeq.map(attr => (attr, representativeAttr))
      })

    val nonJoinAttr2RepresentativeAttr = children
      .flatMap(_.output)
      .filterNot(attr => joinAttr2RepresentativeAttr.contains(attr))
      .map(f => (f, f))

    val attr2RepresentativeAttr = AttributeMap(
      joinAttr2RepresentativeAttr.toSeq ++ nonJoinAttr2RepresentativeAttr
    )

//    println(s"[debug]:attr2RepresentativeAttr:${attr2RepresentativeAttr}")

    // construct edges and nodes
    val edges = children.map { child =>
      JoinHyperGraphEdge(child.output.map(attr2RepresentativeAttr).toSet, child)
    }

    val nodes = edges.flatMap(_.nodes).distinct

    JoinHyperGraph(nodes.toArray, edges.toArray, attr2RepresentativeAttr)
  }

  def apply(multiwayJoin: MultiwayJoin): JoinHyperGraph =
    apply(multiwayJoin.children, multiwayJoin.conditions)

}
