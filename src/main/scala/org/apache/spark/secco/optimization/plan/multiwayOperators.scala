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
  * @param condition equi-join condition
  * @param property additioal property that describes the join
  * @param mode     execution mode
  */
case class MultiwayJoin(
    children: Seq[LogicalPlan],
    condition: Seq[Expression],
    property: Set[JoinProperty] = Set(EquiJoinProperty),
    mode: ExecMode = ExecMode.Coupled
) extends MultiNode
    with Join {

  val joinType: JoinType = Inner

  override def output: Seq[Attribute] = children.flatMap(_.output)
//  {
//    val attributeBuffer = ArrayBuffer[(Attribute, String)]()
//    children.flatMap(_.output).foreach { attr =>
//      if (!attributeBuffer.map(_._2).contains(attr.name)) {
//        attributeBuffer += ((attr, attr.name))
//      }
//    }
//    attributeBuffer.map(_._1)
//  }

  def duplicatedResolved: Boolean =
    children.combinations(2).forall { children =>
      val left = children(0)
      val right = children(1)
      left.outputSet.intersect(right.outputSet).isEmpty
    }

  /** Test if this multiway join is a cyclic multiway join */
  def isCyclic(): Boolean = hypergraph().isCyclic()

  /** Hypergraph that represents the multiway join
    * @return returns a triplet that contains (hypergraph which represents a multiway natural join, a map from attribute in children
    *         to attributes of the hypergraph, map from seq[attribute] to child logical plan)
    */
  def hypergraph(): JoinHyperGraph = JoinHyperGraph(this)

  override def relationalSymbol: String = s"⋈"
}

/** A [[LogicalPlan]] that pairs up of children's partitions and then perform a series of local computation.
  * We assume the inputs are [[Partition]] or [[Relation]]
  *
  * @param children children logical plans
  * @param localPlan plan of local computations
  */
case class LocalStage(
    children: Seq[LogicalPlan],
    localPlan: LogicalPlan
) extends MultiNode {

  override def mode: ExecMode = localPlan.mode

  /** Merge this localStage with LocalStage in its children */
  def mergeConsecutiveLocalStage(): LocalStage = {
    if (!children.exists(_.isInstanceOf[LocalStage])) {
      this
    } else {
      val LChildren = children.filter(_.isInstanceOf[LocalStage])
      LChildren.foldLeft(this)((localStage, childLocalStage) =>
        localStage.merge(childLocalStage.asInstanceOf[LocalStage])
      )
    }
  }

  /** Unbox the localPlan by replacing all placeHolder by actual operator */
  def unboxedPlan(): LogicalPlan = {
    localPlan transform { case placeholder: PlaceHolder =>
      LocalStage.placeHolders2Child(placeholder, children)
    }
  }

  /** Recouple the communication and computation operators */
  def recoupledPlan(): LogicalPlan = {
    unboxedPlan() transform {
      case p: Partition => {
        if (p.child.isInstanceOf[LocalStage]) {
          p.child.asInstanceOf[LocalStage].unboxedPlan()
        } else {
          p.child
        }
      }
      case l: LocalStage => l.unboxedPlan()
    }
  }

  /** Merge two consecutive LocalStage into one */
  private def merge(localStage2Merge: LocalStage): LocalStage = {
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
        case l: LocalStage if l.fastEquals(localStage2Merge) =>
          unboxedLocalPlan2Merge
      }

    LocalStage.box(
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

object LocalStage {

  /** box local computations into LocalStage by replacing all children shown in unboxedLocalPlan into placeHolders */
  def box(
      children: Seq[LogicalPlan],
      unboxedLocalPlan: LogicalPlan
  ): LocalStage = {
    assert(
      unboxedLocalPlan.mode == ExecMode.Computation || unboxedLocalPlan.mode == ExecMode.DelayComputation,
      s"localPlan's mode must be ${ExecMode.Computation} or ${ExecMode.DelayComputation}"
    )
    val localPlan = unboxedLocalPlan transform { case childPlan: LogicalPlan =>
      child2PlaceHolder(childPlan, children)
    }

    LocalStage(children, localPlan)
  }

  /** unbox localStage into local computations */
  def unbox(localStage: LocalStage): LogicalPlan = {
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
