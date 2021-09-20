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

abstract class MultiNode extends LogicalPlan {}

/**
  * An operator that perform cartesian product between [[children]]
  * @param children children logical plan
  * @param mode execution mode: local or global
  */
case class CartesianProduct(children: Seq[LogicalPlan], mode: ExecMode)
    extends MultiNode {

  /** The output attributes */
  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct

  override def relationalSymbol: String = s"⨉"
}

object JoinType extends Enumeration {
  type JoinType = Value
  val Inner, // inner join between two relations
  LeftOuter, // left outer join between two relations
  RightOuter, // right outer join between two relations
  FullOuter, // full outer join between two relations
  Natural, // natural join between relations (possibly more than 2)
  GHD, // natural join inside GHD node
  PKFK, // primary key foreign key natural join
  FKFK, // foreign key foreign key natural join
  GHDFKFK //this join types are subType of natural join
  = Value
}

/**
  * An operators that joins [[children]]
  *
  * @param children children logical plan
  * @param joinType select from
  *                 ([[JoinType.PKFK]]: primary key foreign key natural join,
  *                 [[JoinType.FKFK]]: foreign key foreign key natural join,
  *                 [[JoinType.GHD]]: natural join inside GHD node,
  *                 [[JoinType.GHDFKFK]]: natural join between GHD node,
  *                 [[JoinType.Natural]]: natural join
  * @param mode     execution mode
  */
case class Join(
    children: Seq[LogicalPlan],
    joinType: JoinType = Natural,
    mode: ExecMode = ExecMode.Coupled,
    joinCondition: Seq[Expression] = Seq()
) extends MultiNode {

  /** The output attributes */
  override def outputOld: Seq[String] = children.flatMap(_.outputOld).distinct

  override def output: Seq[Attribute] = {
    if (joinType == Natural) {
      val attributeBuffer = ArrayBuffer[(Attribute, String)]()
      children.flatMap(_.output).foreach { attr =>
        if (!attributeBuffer.map(_._2).contains(attr.name)) {
          attributeBuffer += ((attr, attr.name))
        }
      }
      attributeBuffer.map(_._1)
    } else {
      children.flatMap(_.output)
    }

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

case class Union(children: Seq[LogicalPlan], mode: ExecMode) extends MultiNode {

  /** The output attributes */
  override def outputOld: Seq[String] = children.head.outputOld

  override def relationalSymbol: String = s"⋃"
}

/**
  * An operator that embed a series of local computation.
  * We assume the inputs are partitions,
  * and there is an implicit pair operator that pairs them up.
  *
  * @param children children logical plans
  * @param localPlan plan of local computations
  */
case class LocalStage(
    children: Seq[LogicalPlan],
    localPlan: LogicalPlan
) extends MultiNode {

  override def mode: ExecMode = localPlan.mode

  /** The output attributes */
  override def outputOld: Seq[String] = {
    localPlan.outputOld
  }

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
    localPlan transform {
      case placeholder: PlaceHolder =>
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
    val localPlan = unboxedLocalPlan transform {
      case childPlan: LogicalPlan => child2PlaceHolder(childPlan, children)
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
      PlaceHolder(posOf(planList, childPlan), childPlan.outputOld)
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

  /** The output attributes */
  override def outputOld: Seq[String] = Seq()
}

///**
//  * An operator that embed a series of local computation.
//  * We assume the inputs are partitions,
//  * and there is an implicit pair operator that pairs them up.
//  *
//  * @param children children logical plans
//  * @param innerTreeNodes a sequence of local computation
//  */
//case class LocalStage(
//                       children: Seq[LogicalPlan],
//                       innerTreeNodes: Seq[LogicalPlan],
//                       mode: ExecMode = ExecMode.Computation
//                     ) extends MultiNode {
//
//  /** The root plan of [[innerTreeNodes]] */
//  lazy val rootPlan = {
//    innerTreeNodes.find { plan1 =>
//      innerTreeNodes.diff(Seq(plan1)).forall { plan2 =>
//        plan2.find(plan3 => plan3 == plan1).isEmpty
//      }
//    }.get
//  }
//
//  /** The output attributes */
//  override def outputOld: Seq[String] = {
//    rootPlan.outputOld
//  }
//
//  /** Merge two consecutive LOp into one */
//  def merge(lopToMerge: LocalStage): LocalStage = {
//    assert(
//      children.contains(lopToMerge),
//      "lopToMerge should be a children of this LOp"
//    )
//
//    //TODO: add comment
//    val unboxedLopToMergeSubtreeNodes = lopToMerge.innerTreeNodes.map(plan =>
//      plan transform {
//        case placeholder: PlaceHolder =>
//          LocalStage.placeHolders2Child(
//            placeholder,
//            lopToMerge.children
//          )
//      }
//    )
//
//    val unboxedLopToMergeRoot = lopToMerge.rootPlan transform {
//      case placeholder: PlaceHolder =>
//        LocalStage.placeHolders2Child(
//          placeholder,
//          lopToMerge.children
//        )
//    }
//
//    val unboxedSubtreeNodes = innerTreeNodes
//      .map(plan =>
//        plan transform {
//          case placeholder: PlaceHolder =>
//            LocalStage.placeHolders2Child(placeholder, children)
//        }
//      )
//      .map { plan =>
//        plan transform {
//          case l: LocalStage if l.fastEquals(lopToMerge) =>
//            unboxedLopToMergeRoot
//        }
//      }
//
//    val newSubTreeNodes =
//      unboxedSubtreeNodes ++ unboxedLopToMergeSubtreeNodes
//
//    LocalStage(
//      children.diff(Seq(lopToMerge)) ++ lopToMerge.children,
//      newSubTreeNodes
//    )
//  }
//
//  override def argString: String = s"[${rootPlan.relationalString}]"
//
//}
//
//object LocalStage {
//  def apply(
//             children: Seq[LogicalPlan],
//             innerTreeNodes: Seq[LogicalPlan]
//           ): LocalStage = {
//    val newInnerTreeNodes = innerTreeNodes.map { plan =>
//      plan transform {
//        case childPlan: LogicalPlan => child2PlaceHolder(childPlan, children)
//      }
//    }
//
//    LocalStage(children, newInnerTreeNodes, ExecMode.Computation)
//  }
//
//  /** convert the child to placeholder logical plan */
//  def child2PlaceHolder(
//                         childPlan: LogicalPlan,
//                         planList: Seq[LogicalPlan]
//                       ): LogicalPlan = {
//    if (posOf(planList, childPlan) != -1) {
//      PlaceHolder(posOf(planList, childPlan), childPlan.outputOld)
//    } else {
//      childPlan
//    }
//  }
//
//  /** convert the placeholder logical plan to child */
//  def placeHolders2Child(
//                          childPlan: LogicalPlan,
//                          planList: Seq[LogicalPlan]
//                        ): LogicalPlan = {
//    if (childPlan.isInstanceOf[PlaceHolder]) {
//      planList(childPlan.asInstanceOf[PlaceHolder].pos)
//    } else {
//      childPlan
//    }
//  }
//
//}
