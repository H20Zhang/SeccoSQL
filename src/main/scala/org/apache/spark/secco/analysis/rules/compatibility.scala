package org.apache.spark.secco.analysis.rules

import org.apache.spark.secco.expression.Alias
import org.apache.spark.secco.expression.aggregate.{Count, Max, Min, Sum}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{Aggregate, Distinct, Project}

//object MakeCompatibleForWith extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = ???
//}
//
//object MakeCompatibleForAggregate extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan.transform {
//      case a @ Aggregate(
//            _,
//            _,
//            _,
//            _,
//            _,
//            groupingExpressions,
//            aggregateExpressions
//          ) =>
//        val attributeExtractRegex = "`(\\w*)`".r
//        val plainTextGroupingList = groupingExpressions
//          .map(_.sql)
//          .map(f => attributeExtractRegex.findFirstMatchIn(f).get.subgroups(0))
//
//        val semiRingExpr =
//          aggregateExpressions.filterNot(f =>
//            plainTextGroupingList.contains(f.name)
//          )
//
//        if (semiRingExpr.size != 1)
//          throw new Exception(
//            s"only allow 1 semiRing, current semiRingExpr:${semiRingExpr}"
//          )
//        val (sumOp, addExpr) = semiRingExpr.head match {
//          case Alias(Count(x), _) => ("count", x.sql)
//          case Alias(Sum(x), _)   => ("sum", x.sql)
//          case Alias(Min(x), _)   => ("min", x.sql)
//          case Alias(Max(x), _)   => ("max", x.sql)
//        }
//
//        //TODO: also need to consider the case of expr for iterative query
//        val attrOpt = attributeExtractRegex.findFirstMatchIn(addExpr)
//
//        if (attrOpt.isDefined) {
//          val attr = attrOpt.get.subgroups(0)
//
//          a.copy(
//            groupingListOld = plainTextGroupingList,
//            semiringListOld = (sumOp, attr),
//            groupingExpressions = Seq(),
//            aggregateExpressions = Seq()
//          )
//        } else {
//          throw new Exception("not supported aggregate types")
//        }
//    }
//}
//
//object MakeCompatibleForProject extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan.transform {
//      case Distinct(p @ Project(_, _, _, projectionList), _) =>
//        p.copy(
//          projectionListOld = projectionList.map(_.name),
//          projectionList = Seq()
//        )
//      case p @ Project(child, _, _, projectionList)
//          if projectionList
//            .map(_.name)
//            .toSet == child.output.map(_.name).toSet =>
//        //TODO: this part is only a temporary solution
//        p.copy(
//          projectionListOld = projectionList.map(_.name),
//          projectionList = Seq()
//        )
//    }
//}
