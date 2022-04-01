package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.catalog.TableIdentifier
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.expression.{
  Attribute,
  AttributeReference,
  EqualTo,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.util.ghd.{JoinHyperGraph}
import org.apache.spark.secco.util.`extension`.SeqExtension.posOf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/* ---------------------------------------------------------------------------------------------------------------------
 * This file contains logical plans with multiple children, i.e., n > 2.
 *
 * 0.  MultiNode: base class of logical plan with multiple children.
 * 1.  Union: logical plan that union results of multiple children.
 * 1.  MultiwayNaturalJoin: logical plan that performs multiway natural join between children.
 * 2.  With: logical plan that represent CTE(common table expressions).
 *
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A [[LogicalPlan]] with multiple children */
abstract class MultiNode extends LogicalPlan {}

/** A [[LogicalPlan]] that computes the union of results of children.
  * @param mode execution mode
  */
case class Union(children: Seq[LogicalPlan], mode: ExecMode = ExecMode.Coupled)
    extends MultiNode {

  //TODO: ensure children are compatible.

  override def primaryKey: Seq[Attribute] = children.head.primaryKey

  override def output: Seq[Attribute] = children.head.output

  override def relationalSymbol: String = s"⋃"
}

/** A [[LogicalPlan]] that performs multiway equi-join between [[children]].
  *
  * @param children children logical plan
  * @param conditions equi-join condition
  * @param property additioal property that describes the join
  * @param mode     execution mode
  */
case class MultiwayJoin(
    children: Seq[LogicalPlan],
    conditions: Seq[Expression],
    property: Set[JoinProperty] = Set(EquiJoinProperty),
    mode: ExecMode = ExecMode.Coupled
) extends MultiNode
    with Join {

  /** Join Type. */
  val joinType: JoinType = NaturalJoin(Inner)

  /** Return Hypergraph that represents the multiway join. */
  lazy val hypergraph: JoinHyperGraph = JoinHyperGraph(this)

  /** The representative attributes for each set of attributes related by EqualTo in `condition`. */
  lazy val repAttrs: Seq[Attribute] =
    hypergraph.attr2RepAttr.values.toSeq.distinct

  /** The map from representative attribute to attributes it represents. */
  lazy val repAttr2Attr: AttributeMap[Array[Attribute]] = {

    val repAttr2AttrBuilder =
      mutable.HashMap[Attribute, ArrayBuffer[Attribute]]()

    hypergraph.attr2RepAttr.foreach { case (attr, repAttr) =>
      val attrArray = repAttr2AttrBuilder.getOrElse(repAttr, ArrayBuffer())
      attrArray += attr
      repAttr2AttrBuilder(repAttr) = attrArray
    }

    AttributeMap(repAttr2AttrBuilder.map(f => (f._1, f._2.toArray)).toSeq)
  }

  /** A simple heuristic for computing the attribute order. */
  private val simpleAttributeOrder = {
    assert(
      hypergraph.edges.nonEmpty,
      "HyperGraph of the Multiway join must be non-empty."
    )
    // Follow the edges of hypergraph to determine attributeOrder for representative attributes.
    val edges = hypergraph.edges
    var curEdge = edges.head
    val remainingEdges = mutable.HashSet(edges.drop(1): _*)
    val repAttributeOrderBuilder = ArrayBuffer(curEdge.attrs.toSeq: _*)

    while (remainingEdges.nonEmpty) {

      // Find edges in remaining edges that are adjacent to curEdge.
      val newCurEdge = remainingEdges.find(edge =>
        edge.attrs.intersect(curEdge.attrs).nonEmpty
      ) match {
        case Some(newCurEdge) => newCurEdge
        case None             => remainingEdges.head
      }

      // Remove newly traversed edge.
      remainingEdges.remove(newCurEdge)

      // Add new attributes to attribute order
      repAttributeOrderBuilder ++= newCurEdge.attrs.toSeq.diff(
        repAttributeOrderBuilder
      )
    }

    // Propagate the order of representative attributes to all attributes.
    repAttributeOrderBuilder.flatMap(repAttr => repAttr2Attr(repAttr)).toArray

  }

  def setAttributeOrder(attrOrder: Seq[Attribute]): Unit = {
    _attributeOrder = Some(attrOrder)
  }

  private var _attributeOrder: Option[Seq[Attribute]] = None

  /** The optimized attribute order. */
  def attributeOrder: Seq[Attribute] = {
    _attributeOrder.getOrElse(simpleAttributeOrder)
  }

  /** The relative optimized attributes order for the given attributes. */
  def FindRelativeAttributeOrder(attrs: Seq[Attribute]): Seq[Attribute] =
    attributeOrder.filter { attr =>
      attrs.contains(attr)
    }

  override def output: Seq[Attribute] = {
    attributeOrder
  }

  def duplicatedResolved: Boolean =
    children.combinations(2).forall { children =>
      val left = children(0)
      val right = children(1)
      left.outputSet.intersect(right.outputSet).isEmpty
    }

  /** Test if this multiway join is a cyclic multiway join */
  def isCyclic(): Boolean = hypergraph.isCyclic()

  override def relationalSymbol: String = s"⋈"
}

/** A [[LogicalPlan]] that pairs up of children's partitions and then perform a series of local computation.
  * We assume the inputs are [[Partition]] or [[Relation]]
  *
  * @param children children logical plans
  * @param localPlan plan of local computations
  */
case class PairThenCompute(
    children: Seq[LogicalPlan],
    localPlan: LogicalPlan
) extends MultiNode {

  override def mode: ExecMode = localPlan.mode

  /** Merge this PairThenCompute operator with other PairThenComputer operators in its children. */
  def mergeConsecutiveLocalStage(): PairThenCompute = {
    if (!children.exists(_.isInstanceOf[PairThenCompute])) {
      this
    } else {
      val LChildren = children.filter(_.isInstanceOf[PairThenCompute])
      LChildren.foldLeft(this)((localStage, childLocalStage) =>
        localStage.merge(childLocalStage.asInstanceOf[PairThenCompute])
      )
    }
  }

  /** Unbox the localPlan by replacing all placeHolder by actual operator. */
  def unboxedPlan(): LogicalPlan = {
    localPlan transform { case placeholder: PlaceHolder =>
      PairThenCompute.placeHolders2Child(placeholder, children)
    }
  }

  //TODO: This function has bug.
  // It cannot correctly set the recoupled plan's operator execution mode
  // to ExecMode.Coupled.
  /** Unbox the localPlan then, recouple the communication and computation operators into operators. */
  def recoupledPlan(): LogicalPlan = {
    unboxedPlan() transform {
      case p: Partition => {
        if (p.child.isInstanceOf[PairThenCompute]) {
          p.child.asInstanceOf[PairThenCompute].unboxedPlan()
        } else {
          p.child
        }
      }
      case l: PairThenCompute => l.unboxedPlan()
    }
  }

  /** Merge two consecutive LocalStage into one */
  private def merge(localStage2Merge: PairThenCompute): PairThenCompute = {
    assert(
      children.contains(localStage2Merge),
      "lopToMerge should be a children of this LOp"
    )

    //unbox self's localPlan
    val unboxedLocalPlan = unboxedPlan()

    //unbox child's localPlan
    val unboxedLocalPlan2Merge = localStage2Merge.unboxedPlan()

    //replace localStage2Merge by unboxed local plan
    val newUnboxedLocalPlan =
      unboxedLocalPlan.transform {
        case l: PairThenCompute if l.fastEquals(localStage2Merge) =>
          unboxedLocalPlan2Merge
      }

    PairThenCompute.box(
      children.diff(Seq(localStage2Merge)) ++ localStage2Merge.children,
      newUnboxedLocalPlan
    )
  }

  override def argString: String = {
    mode match {
      case org.apache.spark.secco.optimization.ExecMode.Computation =>
        s"[${localPlan.relationalString}]"
      case org.apache.spark.secco.optimization.ExecMode.DelayComputation =>
        s"[Delay ${localPlan.relationalString}]"
      case _ => throw new Exception("not supported ExecMode in LocalStage")
    }
  }

  /** The output attributes */
  override def output: Seq[Attribute] = localPlan.output
}

object PairThenCompute {

  /** box local computations into LocalStage by replacing all children shown in unboxedLocalPlan into placeHolders */
  def box(
      children: Seq[LogicalPlan],
      unboxedLocalPlan: LogicalPlan
  ): PairThenCompute = {
    assert(
      unboxedLocalPlan.mode == ExecMode.Computation || unboxedLocalPlan.mode == ExecMode.DelayComputation,
      s"localPlan's mode must be ${ExecMode.Computation} or ${ExecMode.DelayComputation}"
    )
    val localPlan = unboxedLocalPlan transform { case childPlan: LogicalPlan =>
      child2PlaceHolder(childPlan, children)
    }

    PairThenCompute(children, localPlan)
  }

  /** unbox localStage into local computations */
  def unbox(localStage: PairThenCompute): LogicalPlan = {
    localStage.unboxedPlan()
  }

  /** convert the child to placeholder logical plan */
  private def child2PlaceHolder(
      childPlan: LogicalPlan,
      planList: Seq[LogicalPlan]
  ): LogicalPlan = {
    if (posOf(planList, childPlan) != -1) {
      PlaceHolder(posOf(planList, childPlan), childPlan.output)
    } else {
      childPlan
    }
  }

  /** convert the placeholder logical plan to child */
  private def placeHolders2Child(
      childPlan: LogicalPlan,
      planList: Seq[LogicalPlan]
  ): LogicalPlan = {
    if (childPlan.isInstanceOf[PlaceHolder]) {
      planList(childPlan.asInstanceOf[PlaceHolder].pos)
    } else {
      childPlan
    }
  }

}

case class WithElement(
    name: TableIdentifier,
    schema: Option[Seq[Attribute]],
    plan: LogicalPlan
)

/** A [[LogicalPlan]] that represents CTE(common table expression)
  * @param recursive numbers of iterations
  * @param query query to be iteratively computed
  * @param withList schema of the temporary tables
  * @param withListQueries [[LogicalPlan]] of the temporary tables.
  */
case class With(
    query: LogicalPlan,
    withList: Seq[WithElement],
    recursive: Option[Option[Int]]
) extends LogicalPlan {

  override def output = query.output

  def children = query +: withList.map(_.plan)

  override def simpleString =
    "With " + withList
      .map { withElem =>
        withElem.name + withElem.schema
          .map(_.mkString("(", ", ", ")"))
          .getOrElse("")
      }
      .mkString("[", ", ", "]")

  override def mode: ExecMode = ExecMode.Atomic
}
