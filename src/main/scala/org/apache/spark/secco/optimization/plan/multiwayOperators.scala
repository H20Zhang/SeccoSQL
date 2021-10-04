package org.apache.spark.secco.optimization.plan

import org.apache.spark.secco.expression.{
  Attribute,
  Expression,
  NamedExpression
}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.plan.JoinType.{JoinType, Natural}
import org.apache.spark.secco.util.`extension`.SeqExtension.posOf
import org.json4s.scalap.scalasig.AttributeInfo

import scala.collection.mutable.ArrayBuffer

/* ---------------------------------------------------------------------------------------------------------------------
 * This file contains logical plans with multiple children, i.e., n > 2.
 *
 * 0.  MultiNode: base class of logical plan with multiple children.
 * 1.  MultiwayNaturalJoin: logical plan that performs multiway natural join between children.
 * 2.  With: logical plan that represent CTE(common table expressions).
 *
 * ---------------------------------------------------------------------------------------------------------------------
 */

/** A [[LogicalPlan]] with multiple children */
abstract class MultiNode extends LogicalPlan {}

/** A [[LogicalPlan]] that performs multiway natural join between [[children]]
  *
  * @param children children logical plan
  * @param joinType join type = Natural
  * @param mode     execution mode
  */
case class MultiwayNaturalJoin(
    children: Seq[LogicalPlan],
    joinType: JoinType = Natural,
    mode: ExecMode = ExecMode.Coupled
) extends MultiNode {

  override def output: Seq[Attribute] = {
    val attributeBuffer = ArrayBuffer[(Attribute, String)]()
    children.flatMap(_.output).foreach { attr =>
      if (!attributeBuffer.map(_._2).contains(attr.name)) {
        attributeBuffer += ((attr, attr.name))
      }
    }
    attributeBuffer.map(_._1)
  }

  def duplicatedResolved: Boolean =
    children.combinations(2).forall { children =>
      val left = children(0)
      val right = children(1)
      left.outputSet.intersect(right.outputSet).isEmpty
    }

//  //warning: this method assume we are handling natural join
//  override def resolveAttributeByChildren(
//      nameParts: Seq[String]
//  ): Option[NamedExpression] = {
//    if (joinType == Undefined) {
//      val attributeBuffer = ArrayBuffer[(Attribute, String)]()
//      children.flatMap(_.output).foreach { attr =>
//        if (!attributeBuffer.map(_._2).contains(attr.name)) {
//          attributeBuffer += ((attr, attr.name))
//        }
//      }
//      resolveAttribute(nameParts, attributeBuffer.map(_._1))
//    } else {
//      super.resolveAttributeByChildren(nameParts)
//    }
//
//  }

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

/** A [[LogicalPlan]] that represents CTE(common table expression)
  * @param recursive numbers of iterations
  * @param query query to be iteratively computed
  * @param withList schema of the temporary tables
  * @param withListQueries [[LogicalPlan]] of the temporary tables.
  */
case class With(
    recursive: Option[Option[Int]],
    query: LogicalPlan,
    withList: Seq[(String, Option[Seq[String]])],
    withListQueries: Seq[LogicalPlan]
) extends LogicalPlan {
  override def output = query.output

  def children = query +: withListQueries

  override def simpleString =
    "With " + withList
      .map { withElem =>
        withElem._1 + withElem._2.map(_.mkString("(", ", ", ")")).getOrElse("")
      }
      .mkString("[", ", ", "]")

  override def mode: ExecMode = ExecMode.Atomic
}
