package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan._

/** A set of the basic transformation rules for optimizing the relation algebra
  */

/** A rule that pushes down the rename operator to the leaf */
object PushRenameToLeaf extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp { case r @ Rename(child, attrRenameMap, _) =>
      child.transformUp {
        case s: Filter =>
          val newSelectionExprs = s.selectionExprs.map { case (l, op, r) =>
            if (attrRenameMap.contains(l) && attrRenameMap.contains(r)) {
              (attrRenameMap(l), op, attrRenameMap(r))
            } else if (attrRenameMap.contains(l)) {
              (attrRenameMap(l), op, r)
            } else if (attrRenameMap.contains(r)) {
              (l, op, attrRenameMap(r))
            } else {
              (l, op, r)
            }
          }

          s.copy(selectionExprs = newSelectionExprs)
        case p: Project =>
          val newProjectionList = p.projectionListOld.map { attr =>
            attrRenameMap.getOrElse(attr, attr)
          }
          p.copy(projectionListOld = newProjectionList)
        case t: Transform =>
          val newOutput = t.outputOld.map { attr =>
            attrRenameMap.getOrElse(attr, attr)
          }
          t.copy(outputOld = newOutput)
        case a: Aggregate =>
          val newGroupingList = a.groupingListOld.map { attr =>
            attrRenameMap.getOrElse(attr, attr)
          }

          val funcExpr = a.semiringListOld._2
          val attrExtractRegex = "\\w+".r
          val opExtractRegex = "[\\*|+]".r
          val newAttributes =
            attrExtractRegex.findAllIn(funcExpr).toList.map { attr =>
              attrRenameMap.getOrElse(attr, attr)
            }
          val opStrings = opExtractRegex.findAllIn(funcExpr).toList
          val newFuncExpr = newAttributes.size == 1 match {
            case true => newAttributes(0)
            case false =>
              newAttributes(0) + opStrings
                .zip(newAttributes.drop(1))
                .map(f => f._1 + f._2)
                .mkString("")
          }
          val newSemiringList = (a.semiringListOld._1, newFuncExpr)

          val newProducedOutput = attrRenameMap.getOrElse(
            a.producedOutputOld.head,
            a.producedOutputOld.head
          )
          Aggregate(
            a.child,
            newGroupingList,
            newSemiringList,
            Seq(newProducedOutput),
            a.mode
          )
        case r @ Rename(_, childAttrRenameMap, _) =>
          val newAttrRenameMap = childAttrRenameMap.map { case (key, value) =>
            (
              attrRenameMap.getOrElse(key, key),
              attrRenameMap.getOrElse(value, value)
            )
          }
          r.copy(attrRenameMap = newAttrRenameMap)
        case s: Relation
            if s.outputOld.intersect(attrRenameMap.keys.toSeq).nonEmpty =>
          r.copy(child = s)
        case s: Relation
            if s.outputOld.intersect(attrRenameMap.keys.toSeq).isEmpty =>
          s
      }
    }
}

/** A rule that pushes selection through the join */
object PushSelectionThroughJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case Filter(
            MultiwayNaturalJoin(children, joinType, mode1, _),
            expr,
            mode2,
            _
          ) => {
        val filteredChildren = children.map { child =>
          val output = child.outputOld
          val validSelectionExpr =
            expr.filter(p => output.contains(p._1) && output.contains(p._3))

          if (validSelectionExpr.isEmpty) {
            child
          } else {
            Filter(child, validSelectionExpr, mode2)
          }
        }

        MultiwayNaturalJoin(filteredChildren, joinType, mode1)
      }
    }
}

/** A rule that pushes projection throught the join */
object PushProjectionThroughJoin extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case p @ Project(
            MultiwayNaturalJoin(children, joinType, mode1, _),
            expr,
            mode2,
            _
          ) => {

        //find attributes involved in join
        val joinAttributes = children
          .flatMap(_.outputOld)
          .groupBy(f => f)
          .map(f => (f._1, f._2.size))
          .filter(f => f._2 > 1)
          .map(_._1)
          .toSeq
          .distinct

        //push projection down to the children
        val projectedChildren = children.map { child =>
          val output = child.outputOld
          val joinOutput = joinAttributes.intersect(output)
          val outputToPreserve = output.intersect(expr) ++ joinOutput

          Project(child, outputToPreserve.distinct, mode2)
        }

        //check if the last projection is necessary
        if (projectedChildren.forall(p => p.outputOld == expr)) {
          MultiwayNaturalJoin(projectedChildren, joinType, mode1)
        } else {
          Project(
            MultiwayNaturalJoin(projectedChildren, joinType, mode1),
            expr,
            mode2
          )
        }
      }
    }
}

/** A rule that pushes selection through the union */
object PushSelectionThroughUnion extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case Filter(Union(children, mode1), selectionExprs, mode2, _) => {
        Union(children.map(f => Filter(f, selectionExprs, mode2)), mode1)
      }
    }
}

/** A rule that pushes selection through the union */
object PushProjectionThroughUnion extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case Project(
            Union(children, mode1),
            projectionList,
            mode2,
            _
          ) => {
        Union(
          children.map(f => Project(f, projectionList, mode2)),
          mode1
        )
      }
    }
}

/** A rule that pushes aggregation along the GHD Tree */
object PushSemiringAggregationAlongGHDTree extends Rule[LogicalPlan] {

  // handle the case where aggregation involves count
  private def handleCount(
      aggregate: Aggregate,
      plans: Seq[LogicalPlan],
      groupingList: Seq[String],
      semiringList: (String, String)
  ): LogicalPlan = {
    val lChild = plans(0)
    val rChild = plans(1)
    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
    val semiringAttribute = semiringList._2

    def aggregatedPlan(plan: LogicalPlan) =
      (groupingList ++ joinAttributes).distinct.toSet == plan.outputOld.toSet match {
        case true => { plan }
        case false => {
          Aggregate(
            plan,
            (groupingList ++ joinAttributes).intersect(plan.outputOld).distinct,
            ("count", s"${semiringAttribute}"),
            Seq(),
            ExecMode.Coupled
          )
        }
      }

    val lPlan = aggregatedPlan(lChild)
    val rPlan = aggregatedPlan(rChild)

    if (lPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("sum", s"${rPlan.producedOutputOld.head}"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else if (rPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("sum", s"${lPlan.producedOutputOld.head}"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else if (
      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
    ) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("count", s"${semiringAttribute}"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        (
          "sum",
          s"${lPlan.producedOutputOld.head}*${rPlan.producedOutputOld.head}"
        ),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    }
  }

  // handle the case where aggregation involves min or max
  private def handleMinMax(
      plans: Seq[LogicalPlan],
      groupingList: Seq[String],
      semiringList: (String, String),
      producedOutput: Seq[String],
      isMin: Boolean
  ): LogicalPlan = {

    val semiringOp = isMin match {
      case true  => "min"
      case false => "max"
    }

    val lChild = plans(0)
    val rChild = plans(1)
    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
    val semiringAttribute = semiringList._2

    def aggregatedPlan(plan: LogicalPlan) =
      plan.outputOld.contains(semiringAttribute) match {
        case true =>
          Aggregate(
            plan,
            (groupingList ++ joinAttributes).intersect(plan.outputOld),
            semiringList,
            Seq(),
            ExecMode.Coupled
          )
        case false =>
          if (
            (groupingList ++ joinAttributes)
              .intersect(plan.outputOld)
              .toSet == plan.outputOld.toSet
          ) {
            plan
          } else {
            Project(
              plan,
              (groupingList ++ joinAttributes).intersect(plan.outputOld),
              ExecMode.Coupled
            )
          }

      }

    val lPlan = aggregatedPlan(lChild)
    val rPlan = aggregatedPlan(rChild)

    if (lPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        (semiringOp, s"${rPlan.producedOutputOld.head}"),
        producedOutput,
        ExecMode.Coupled
      )
    } else if (rPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        (semiringOp, s"${lPlan.producedOutputOld.head}"),
        producedOutput,
        ExecMode.Coupled
      )
    } else if (
      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
    ) {
      throw new Exception("error occurs when pushing min/max down")
    } else {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        (semiringOp, s"${lPlan.producedOutputOld.head}"),
        producedOutput,
        ExecMode.Coupled
      )
    }
  }

  // handle the case where aggregation involves sum
  private def handleSum(
      aggregate: Aggregate,
      plans: Seq[LogicalPlan],
      groupingList: Seq[String],
      semiringList: (String, String)
  ): LogicalPlan = {
    val lChild = plans(0)
    val rChild = plans(1)
    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
    val semiringAttribute = semiringList._2

    def aggregatedPlan(plan: LogicalPlan) =
      plan.outputOld.contains(semiringAttribute) match {
        case true =>
          Aggregate(
            plan,
            (groupingList ++ joinAttributes).intersect(plan.outputOld),
            semiringList,
            Seq(),
            ExecMode.Coupled
          )
        case false =>
          if (
            (groupingList ++ joinAttributes)
              .intersect(plan.outputOld)
              .toSet != plan.outputOld.toSet
          ) {
            Aggregate(
              plan,
              (groupingList ++ joinAttributes).intersect(plan.outputOld),
              ("count", "*"),
              Seq(),
              ExecMode.Coupled
            )
          } else {
            plan
          }
      }

    val lPlan = aggregatedPlan(lChild)
    val rPlan = aggregatedPlan(rChild)

    //DEBUG
    //    println(lPlan.producedOutput)
    //    println(rPlan.producedOutput)
    //    println(lPlan.treeString)
    //    println(rPlan.treeString)

    if (lPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("sum", s"${rPlan.producedOutputOld.head}"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else if (rPlan.producedOutputOld.isEmpty) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("sum", s"${lPlan.producedOutputOld.head}"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else if (
      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
    ) {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        ("count", s"*"),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    } else {
      Aggregate(
        MultiwayNaturalJoin(
          lPlan :: rPlan :: Nil,
          JoinType.GHDFKFK,
          ExecMode.Coupled
        ),
        groupingList,
        (
          "sum",
          s"${lPlan.producedOutputOld.head}*${rPlan.producedOutputOld.head}"
        ),
        aggregate.producedOutputOld,
        ExecMode.Coupled
      )
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform {
      case a @ Aggregate(
            MultiwayNaturalJoin(children, JoinType.GHDFKFK, mode1, _),
            groupingList,
            semiringList,
            producedOutput,
            mode2,
            _,
            _
          ) => {
        semiringList match {
          case ("min", _) =>
            handleMinMax(
              children,
              groupingList,
              semiringList,
              producedOutput,
              true
            )
          case ("max", _) =>
            handleMinMax(
              children,
              groupingList,
              semiringList,
              producedOutput,
              false
            )
          case ("count", _) =>
            handleCount(a, children, groupingList, semiringList)
          case ("sum", _) => handleSum(a, children, groupingList, semiringList)
        }
      }
    }
}

object CleanRoot extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case r: RootNode =>
      r.child
    }
}
