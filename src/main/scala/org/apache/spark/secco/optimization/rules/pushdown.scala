package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.expression.aggregate.AggregateExpression
import org.apache.spark.secco.expression.{
  Alias,
  And,
  Attribute,
  Expression,
  PredicateHelper
}
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.support.ExtractFiltersAndInnerJoins

import scala.annotation.tailrec

/* This file contains rules for pushing logical operators down.
 *
 *  1. PushDownSelection: push down selection through [[Aggregate]], [[Project]], [[Union]], [[Intersection]], [[Diff]].
 *  2. PushSelectionIntoJoin: push selection into Binary Join (Cross) and make it a theta-join.
 *  3. PushSelectionThroughJoin: push filter condition in selection and join condition in Binary join to children.
 *  4. PushDownProjection: push down projection.
 *  5. PushDownAggregation: push aggregation through GHD. (TBD)
 *  6. PushDownLimit: push down limit. (TBD)
 */

/** A rule that pushes [[Filter]] through many operators, which includes [[Aggregate]], [[Project]], [[Union]],
  * [[Intersection]], [[Except]].
  *
  * Note: push-down for join is considered in [[PushSelectionIntoJoin]]
  */
object PushDownSelection extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // Push down selection through project
    case Filter(
          project @ Project(grandChild, fields, childExecMode),
          condition,
          execMode
        ) if execMode == childExecMode && fields.forall(_.deterministic) =>
      //Create a map of Aliases to their values from the child projection.
      // e.g., 'Select a + b as c, d ...' -> Map(c -> a + b)
      val aliasMap = AttributeMap(fields.collect { case a: Alias =>
        (a.toAttribute, a.child)
      })

      project.copy(child =
        Filter(grandChild, replaceAlias(condition, aliasMap))
      )

    // Push down selection through aggregate.
    case filter @ Filter(aggregate: Aggregate, condition, execMode)
        if aggregate.aggregateExpressions.forall(
          _.deterministic
        ) && aggregate.groupingExpressions.nonEmpty =>
      // We just consider attributes exists in aggregate.child, i.e., attribute is not produced in aggregateExpression.
      val aliasMap = AttributeMap(aggregate.aggregateExpressions.collect {
        case a: Alias
            if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })

      val (candidates, nonDeterministic) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(
          aggregate.child.outputSet
        )
      }

      val stayUp = rest ++ nonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate =
          aggregate.copy(child = Filter(aggregate.child, replaced, execMode))

        if (stayUp.isEmpty) newAggregate
        else Filter(newAggregate, stayUp.reduce(And), execMode)
      } else {
        filter
      }

    // Push down selection through set operators.
    case filter @ Filter(setOperator: LogicalPlan, condition, execMode)
        if setOperator.isInstanceOf[Union] || setOperator
          .isInstanceOf[Except] || setOperator.isInstanceOf[Intersection] =>
      val (pushDown, stayUp) =
        splitConjunctivePredicates(condition).partition(_.deterministic)

      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduceLeft(And)
        val output = setOperator.output
        val newGrandChildren = setOperator.children.map { grandChild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandChild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandChild.outputSet))
          Filter(grandChild, newCond, execMode)
        }

        val newSetOperator = setOperator.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(newSetOperator, stayUp.reduceLeft(And), execMode)
        } else {
          newSetOperator
        }
      } else {
        filter
      }
  }
}

/** A rule that pushes selections into join's conditions, which essentially makes the outcome joins, the theta-join. */
object PushSelectionIntoJoin extends Rule[LogicalPlan] with PredicateHelper {

  /** Join a list of plans together and push down the conditions into them.
    *
    * The joined plan are picked from left to right, prefer those has at least one join condition.
    *
    * @param input a list of LogicalPlans to inner join and the type of inner join.
    * @param conditions a list of condition for join.
    */

  @tailrec final def createOrderedJoin(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression],
      mode: ExecMode
  ): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _)         => Cross
      }
      val join = BinaryJoin(
        left,
        right,
        innerJoinType,
        joinConditions.reduceLeftOption(And),
        mode = mode
      )
      if (others.nonEmpty) {
        Filter(join, others.reduceLeft(And))
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(e =>
        e.references.subsetOf(joinedRefs) && canEvaluateWithinJoin(e)
      )
      val joined = BinaryJoin(
        left,
        right,
        innerJoinType,
        joinConditions.reduceLeftOption(And),
        mode = mode
      )

      // should not have reference to same logical plan
      createOrderedJoin(
        Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right),
        others,
        mode
      )
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case p @ ExtractFiltersAndInnerJoins((input, conditions), mode)
        if input.size > 2 && conditions.nonEmpty =>
      val reordered = createOrderedJoin(input, conditions, mode)

      if (sameOutput(p, reordered)) {
        reordered
      } else {
        // Reordering the joins have changed the order of the columns.
        // Inject a projection to make sure we restore to the expected ordering.
        Project(reordered, p.output, mode)
      }
  }

  /** Returns true iff output of both plans are semantically the same, ie.:
    *  - they contain the same number of `Attribute`s;
    *  - references are the same;
    *  - the order is equal too.
    * NOTE: this is copied over from SPARK-25691 from master.
    */
  def sameOutput(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    val output1 = plan1.output
    val output2 = plan2.output
    output1.length == output2.length && output1.zip(output2).forall {
      case (a1, a2) => a1.semanticEquals(a2)
    }
  }
}

/** A rule that pushes selection through the join.
  *
  * Note that By performing [[PushSelectionIntoJoin]] and [[PushDownSelection]], the selection predicates is pushed
  * into join conditions. This rules further pushes join condition and filter condition (Filter above Join) into join's
  * children.
  *
  * We push down for following cases:
  *   - For inner join | left semi-join, we can push down predicates only on left|right side to left|right child.
  *   - For outer join, we can push down join predicates to `Preserved Row Table` side, and push down filter predicates,
  *   to `Null Supplying Table` side.
  *   - For left anti-join, we can only push join predicates to right child, if there exists predicates only on right
  *   side.
  *
  * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more.
  */
object PushSelectionThroughJoin extends Rule[LogicalPlan] with PredicateHelper {

  /** Splits join condition expressions or filter predicates (on a given join's output) into three
    * categories based on the attributes required to evaluate them. Note that we explicitly exclude
    * non-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
    * canEvaluateInRight to prevent pushing these predicates on either side of the join.
    *
    * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
    */
  private def split(
      condition: Seq[Expression],
      left: LogicalPlan,
      right: LogicalPlan
  ) = {
    val (pushDownCandidates, nonDeterministic) =
      condition.partition(_.deterministic)
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    val (rightEvaluateCondition, commonCondition) =
      rest.partition(expr => expr.references.subsetOf(right.outputSet))

    (
      leftEvaluateCondition,
      rightEvaluateCondition,
      commonCondition ++ nonDeterministic
    )
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(
          BinaryJoin(
            left,
            right,
            joinType,
            joinCondition,
            joinProperty,
            childMode
          ),
          filterCondition,
          execMode
        ) if childMode == execMode =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions
            .reduceLeftOption(And)
            .map(Filter(left, _))
            .getOrElse(left)
          val newRight = rightFilterConditions
            .reduceLeftOption(And)
            .map(Filter(right, _))
            .getOrElse(right)
          val (newJoinConditions, others) =
            commonFilterCondition.partition(canEvaluateWithinJoin)
          val newJoinCond =
            (newJoinConditions ++ joinCondition).reduceLeftOption(And)

          val join = BinaryJoin(
            newLeft,
            newRight,
            joinType,
            newJoinCond,
            mode = execMode
          )
          if (others.nonEmpty) {
            Filter(join, others.reduceLeft(And))
          } else {
            join
          }
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions
            .reduceLeftOption(And)
            .map(Filter(right, _))
            .getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin =
            BinaryJoin(
              newLeft,
              newRight,
              RightOuter,
              newJoinCond,
              mode = execMode
            )

          (leftFilterConditions ++ commonFilterCondition)
            .reduceLeftOption(And)
            .map(Filter(newJoin, _))
            .getOrElse(newJoin)
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions
            .reduceLeftOption(And)
            .map(Filter(left, _))
            .getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin =
            BinaryJoin(
              newLeft,
              newRight,
              joinType,
              newJoinCond,
              mode = execMode
            )

          (rightFilterConditions ++ commonFilterCondition)
            .reduceLeftOption(And)
            .map(Filter(newJoin, _))
            .getOrElse(newJoin)
        case FullOuter       => f // DO Nothing for Full Outer Join
        case NaturalJoin(_)  => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    case j @ BinaryJoin(
          left,
          right,
          joinType,
          joinCondition,
          joinProperties,
          execMode
        ) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(
          joinCondition.map(splitConjunctivePredicates).getOrElse(Nil),
          left,
          right
        )

      joinType match {
        case _: InnerLike | LeftSemi =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions
            .reduceLeftOption(And)
            .map(Filter(left, _))
            .getOrElse(left)
          val newRight = rightJoinConditions
            .reduceLeftOption(And)
            .map(Filter(right, _))
            .getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          BinaryJoin(newLeft, newRight, joinType, newJoinCond, mode = execMode)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions
            .reduceLeftOption(And)
            .map(Filter(left, _))
            .getOrElse(left)
          val newRight = right
          val newJoinCond =
            (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          BinaryJoin(
            newLeft,
            newRight,
            RightOuter,
            newJoinCond,
            mode = execMode
          )
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions
            .reduceLeftOption(And)
            .map(Filter(right, _))
            .getOrElse(right)
          val newJoinCond =
            (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          BinaryJoin(newLeft, newRight, joinType, newJoinCond)
        case FullOuter       => j
        case NaturalJoin(_)  => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/** A rule that pushes projection down.
  *
  * We push down if Projection's child contains cases:
  *
  *   - For Union, we can completely push down projection list to all children.
  *   - For Aggregate, we can use projection to eliminate some aggregateExpressions.
  *   - For Project, we can use projection to eliminate some projectionList.
  *   - For Intersection, Diff, Distinct, we cannot push down projection.
  *   - By default, we can partially push down projection to prune unused columns,
  *   i.e., not in reference of projection's expression.
  */
object PushDownProjection extends Rule[LogicalPlan] {

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(c, c.output.filter(allReferences.contains), c.mode)
    } else {
      c
    }

  /** The Project before Filter is not necessary but conflict with PushPredicatesThroughProject,
    * so remove it. Since the Projects have been added top-down, we need to remove in bottom-up
    * order, otherwise lower Projects can be missed.
    */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      case p1 @ Project(f @ Filter(p2 @ Project(child, _, _), _, _), _, _)
          if p2.outputSet.subsetOf(child.outputSet) =>
        p1.copy(child = f.copy(child = child))
    }

  private def sameOutput(
      output1: Seq[Attribute],
      output2: Seq[Attribute]
  ): Boolean =
    output1.size == output2.size &&
      output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))

  /** Maps Attributes from the left side to the corresponding Attribute on the right side.
    */
  private def buildRewrites(
      left: LogicalPlan,
      right: LogicalPlan
  ): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /** Rewrites an expression so that it can be pushed to the right side of a
    * Union or Except operator. This method relies on the fact that the output attributes
    * of a union/intersect/except are always equal to the left child's output.
    */
  private def pushToRight[A <: Expression](
      e: A,
      rewrites: AttributeMap[Attribute]
  ) = {
    val result = e transform { case a: Attribute =>
      rewrites(a)
    } match {
      // Make sure exprId is unique in each child of Union.
      case Alias(child, alias) => Alias(child, alias)()
      case other               => other
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    removeProjectBeforeFilter(plan) transform {

      // Push down for UNION ALL.
      case p @ Project(u @ Union(children, childMode), projectList, mode)
          if mode == childMode =>
        assert(children.nonEmpty)
        // Completely Push down deterministic projection through UNION ALL.
        if (projectList.forall(_.deterministic)) {
          val newFirstChild = Project(children.head, projectList)
          val newOtherChildren = children.tail.map { child =>
            val rewrites = buildRewrites(children.head, child)
            Project(child, projectList.map(pushToRight(_, rewrites)))
          }
          Union(newFirstChild +: newOtherChildren)
        } else {
          // partially ush down projection through UNION ALL.
          if ((u.outputSet -- p.references).nonEmpty) {
            val firstChild = u.children.head
            val newOutput = prunedChild(firstChild, p.references).output
            // pruning the columns of all children based on the pruned first child.
            val newChildren = u.children.map { p =>
              val selected = p.output.zipWithIndex
                .filter { case (a, i) =>
                  newOutput.contains(firstChild.output(i))
                }
                .map(_._1)
              Project(p, selected, mode)
            }
            p.copy(child = u.withNewChildren(newChildren))
          } else {
            p
          }
        }

      // Prunes the unused columns from project list of Project/Aggregate
      case p @ Project(p2: Project, _, _)
          if (p2.outputSet -- p.references).nonEmpty =>
        p.copy(child =
          p2.copy(projectionList =
            p2.projectionList.filter(p.references.contains)
          )
        )
      case p @ Project(a: Aggregate, _, _)
          if (a.outputSet -- p.references).nonEmpty =>
        p.copy(
          child = a.copy(aggregateExpressions =
            a.aggregateExpressions.filter(p.references.contains)
          )
        )

      // Eliminate no-op Projects
      case p @ Project(child, _, _) if sameOutput(child.output, p.output) =>
        child

      // Can't prune the columns on LeafNode
      case p @ Project(_: LeafNode, _, _) => p

      // Can't prune the columns of Diff
      case p @ Project(_: Except, _, _) => p

      // Can't prune the columns of Intersection
      case p @ Project(_: Intersection, _, _) => p

      // Can't prune the columns of distinct
      case p @ Project(_: Distinct, _, _) => p

      // for all other logical plans that inherits the output from it's children
      case p @ Project(child, _, _) =>
        val required = child.references ++ p.references
        if ((child.inputSet -- required).nonEmpty) {
          val newChildren = child.children.map(c => prunedChild(c, required))
          p.copy(child = child.withNewChildren(newChildren))
        } else {
          p
        }
    }
}

//TODO: make the aggregation push-down work.
/** A rule that pushes aggregation along the GHD Tree */
//object PushDownAggregation extends Rule[LogicalPlan] {
//
//  // handle the case where aggregation involves count
//  private def handleCount(
//      aggregate: Aggregate,
//      plans: Seq[LogicalPlan],
//      groupingList: Seq[String],
//      semiringList: (String, String)
//  ): LogicalPlan = {
//    val lChild = plans(0)
//    val rChild = plans(1)
//    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
//    val semiringAttribute = semiringList._2
//
//    def aggregatedPlan(plan: LogicalPlan) =
//      (groupingList ++ joinAttributes).distinct.toSet == plan.outputOld.toSet match {
//        case true => { plan }
//        case false => {
//          Aggregate(
//            plan,
//            (groupingList ++ joinAttributes).intersect(plan.outputOld).distinct,
//            ("count", s"${semiringAttribute}"),
//            Seq(),
//            ExecMode.Coupled
//          )
//        }
//      }
//
//    val lPlan = aggregatedPlan(lChild)
//    val rPlan = aggregatedPlan(rChild)
//
//    if (lPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("sum", s"${rPlan.producedOutputOld.head}"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else if (rPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("sum", s"${lPlan.producedOutputOld.head}"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else if (
//      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
//    ) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("count", s"${semiringAttribute}"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        (
//          "sum",
//          s"${lPlan.producedOutputOld.head}*${rPlan.producedOutputOld.head}"
//        ),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    }
//  }
//
//  // handle the case where aggregation involves min or max
//  private def handleMinMax(
//      plans: Seq[LogicalPlan],
//      groupingList: Seq[String],
//      semiringList: (String, String),
//      producedOutput: Seq[String],
//      isMin: Boolean
//  ): LogicalPlan = {
//
//    val semiringOp = isMin match {
//      case true  => "min"
//      case false => "max"
//    }
//
//    val lChild = plans(0)
//    val rChild = plans(1)
//    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
//    val semiringAttribute = semiringList._2
//
//    def aggregatedPlan(plan: LogicalPlan) =
//      plan.outputOld.contains(semiringAttribute) match {
//        case true =>
//          Aggregate(
//            plan,
//            (groupingList ++ joinAttributes).intersect(plan.outputOld),
//            semiringList,
//            Seq(),
//            ExecMode.Coupled
//          )
//        case false =>
//          if (
//            (groupingList ++ joinAttributes)
//              .intersect(plan.outputOld)
//              .toSet == plan.outputOld.toSet
//          ) {
//            plan
//          } else {
//            Project(
//              plan,
//              (groupingList ++ joinAttributes).intersect(plan.outputOld),
//              ExecMode.Coupled
//            )
//          }
//
//      }
//
//    val lPlan = aggregatedPlan(lChild)
//    val rPlan = aggregatedPlan(rChild)
//
//    if (lPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        (semiringOp, s"${rPlan.producedOutputOld.head}"),
//        producedOutput,
//        ExecMode.Coupled
//      )
//    } else if (rPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        (semiringOp, s"${lPlan.producedOutputOld.head}"),
//        producedOutput,
//        ExecMode.Coupled
//      )
//    } else if (
//      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
//    ) {
//      throw new Exception("error occurs when pushing min/max down")
//    } else {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        (semiringOp, s"${lPlan.producedOutputOld.head}"),
//        producedOutput,
//        ExecMode.Coupled
//      )
//    }
//  }
//
//  // handle the case where aggregation involves sum
//  private def handleSum(
//      aggregate: Aggregate,
//      plans: Seq[LogicalPlan],
//      groupingList: Seq[String],
//      semiringList: (String, String)
//  ): LogicalPlan = {
//    val lChild = plans(0)
//    val rChild = plans(1)
//    val joinAttributes = lChild.outputOld.intersect(rChild.outputOld)
//    val semiringAttribute = semiringList._2
//
//    def aggregatedPlan(plan: LogicalPlan) =
//      plan.outputOld.contains(semiringAttribute) match {
//        case true =>
//          Aggregate(
//            plan,
//            (groupingList ++ joinAttributes).intersect(plan.outputOld),
//            semiringList,
//            Seq(),
//            ExecMode.Coupled
//          )
//        case false =>
//          if (
//            (groupingList ++ joinAttributes)
//              .intersect(plan.outputOld)
//              .toSet != plan.outputOld.toSet
//          ) {
//            Aggregate(
//              plan,
//              (groupingList ++ joinAttributes).intersect(plan.outputOld),
//              ("count", "*"),
//              Seq(),
//              ExecMode.Coupled
//            )
//          } else {
//            plan
//          }
//      }
//
//    val lPlan = aggregatedPlan(lChild)
//    val rPlan = aggregatedPlan(rChild)
//
//    //DEBUG
//    //    println(lPlan.producedOutput)
//    //    println(rPlan.producedOutput)
//    //    println(lPlan.treeString)
//    //    println(rPlan.treeString)
//
//    if (lPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("sum", s"${rPlan.producedOutputOld.head}"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else if (rPlan.producedOutputOld.isEmpty) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("sum", s"${lPlan.producedOutputOld.head}"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else if (
//      lPlan.producedOutputOld.isEmpty && lPlan.producedOutputOld.isEmpty
//    ) {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        ("count", s"*"),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    } else {
//      Aggregate(
//        MultiwayJoin(
//          lPlan :: rPlan :: Nil,
//          JoinType.GHDFKFK,
//          ExecMode.Coupled
//        ),
//        groupingList,
//        (
//          "sum",
//          s"${lPlan.producedOutputOld.head}*${rPlan.producedOutputOld.head}"
//        ),
//        aggregate.producedOutputOld,
//        ExecMode.Coupled
//      )
//    }
//  }
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case a @ Aggregate(
//            MultiwayJoin(children, JoinType.GHDFKFK, mode1, _),
//            groupingList,
//            semiringList,
//            producedOutput,
//            mode2,
//            _,
//            _
//          ) => {
//        semiringList match {
//          case ("min", _) =>
//            handleMinMax(
//              children,
//              groupingList,
//              semiringList,
//              producedOutput,
//              true
//            )
//          case ("max", _) =>
//            handleMinMax(
//              children,
//              groupingList,
//              semiringList,
//              producedOutput,
//              false
//            )
//          case ("count", _) =>
//            handleCount(a, children, groupingList, semiringList)
//          case ("sum", _) => handleSum(a, children, groupingList, semiringList)
//        }
//      }
//    }
//}

///** A rule that pushes down the rename operator to the leaf */
//@deprecated
//object PushRenameToLeaf extends Rule[LogicalPlan] {
//  override def apply(plan: LogicalPlan): LogicalPlan = ???
//    plan transformUp { case r @ Rename(child, attrRenameMap, _) =>
//      child.transformUp {
//        case s: Filter =>
//          val newSelectionExprs = s.selectionExprs.map { case (l, op, r) =>
//            if (attrRenameMap.contains(l) && attrRenameMap.contains(r)) {
//              (attrRenameMap(l), op, attrRenameMap(r))
//            } else if (attrRenameMap.contains(l)) {
//              (attrRenameMap(l), op, r)
//            } else if (attrRenameMap.contains(r)) {
//              (l, op, attrRenameMap(r))
//            } else {
//              (l, op, r)
//            }
//          }
//
//          s.copy(selectionExprs = newSelectionExprs)
//        case p: Project =>
//          val newProjectionList = p.projectionListOld.map { attr =>
//            attrRenameMap.getOrElse(attr, attr)
//          }
//          p.copy(projectionListOld = newProjectionList)
//        case t: Transform =>
//          val newOutput = t.outputOld.map { attr =>
//            attrRenameMap.getOrElse(attr, attr)
//          }
//          t.copy(outputOld = newOutput)
//        case a: Aggregate =>
//          val newGroupingList = a.groupingListOld.map { attr =>
//            attrRenameMap.getOrElse(attr, attr)
//          }
//
//          val funcExpr = a.semiringListOld._2
//          val attrExtractRegex = "\\w+".r
//          val opExtractRegex = "[\\*|+]".r
//          val newAttributes =
//            attrExtractRegex.findAllIn(funcExpr).toList.map { attr =>
//              attrRenameMap.getOrElse(attr, attr)
//            }
//          val opStrings = opExtractRegex.findAllIn(funcExpr).toList
//          val newFuncExpr = newAttributes.size == 1 match {
//            case true => newAttributes(0)
//            case false =>
//              newAttributes(0) + opStrings
//                .zip(newAttributes.drop(1))
//                .map(f => f._1 + f._2)
//                .mkString("")
//          }
//          val newSemiringList = (a.semiringListOld._1, newFuncExpr)
//
//          val newProducedOutput = attrRenameMap.getOrElse(
//            a.producedOutputOld.head,
//            a.producedOutputOld.head
//          )
//          Aggregate(
//            a.child,
//            newGroupingList,
//            newSemiringList,
//            Seq(newProducedOutput),
//            a.mode
//          )
//        case r @ Rename(_, childAttrRenameMap, _) =>
//          val newAttrRenameMap = childAttrRenameMap.map { case (key, value) =>
//            (
//              attrRenameMap.getOrElse(key, key),
//              attrRenameMap.getOrElse(value, value)
//            )
//          }
//          r.copy(attrRenameMap = newAttrRenameMap)
//        case s: Relation
//            if s.outputOld.intersect(attrRenameMap.keys.toSeq).nonEmpty =>
//          r.copy(child = s)
//        case s: Relation
//            if s.outputOld.intersect(attrRenameMap.keys.toSeq).isEmpty =>
//          s
//      }
//    }
//}
