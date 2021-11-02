package org.apache.spark.secco.analysis.rules

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.analysis.{
  AnalysisException,
  MultiInstanceRelation,
  NoSuchFunctionException,
  NoSuchTableException,
  UnresolvedAlias,
  UnresolvedAttribute,
  UnresolvedFunction,
  UnresolvedRelation
}
import org.apache.spark.secco.catalog.TableIdentifier
import org.apache.spark.secco.expression.{
  Alias,
  And,
  Attribute,
  Coalesce,
  EqualTo,
  Expression,
  NamedExpression,
  Star
}
import org.apache.spark.secco.expression.aggregate.AggregateFunction
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  BinaryJoin,
  FullOuter,
  InnerLike,
  JoinType,
  LeftExistence,
  LeftOuter,
  NaturalJoin,
  Project,
  Relation,
  RightOuter,
  UsingJoin
}

import scala.collection.mutable

/* This file define resolution rules. */

/** Checks whether a function identifier referenced by an [[UnresolvedFunction]] is defined in the
  * function registry. Note that this rule doesn't try to resolve the [[UnresolvedFunction]]. It
  * only performs simple existence check according to the function identifier to quickly identify
  * undefined functions without triggering relation resolution, which may incur potentially
  * expensive partition/schema discovery process in some cases.
  *
  * @see [[ResolveFunctions]]
  */
object LookupFunctions extends Rule[LogicalPlan] {

  def functionRegistry =
    SeccoSession.currentSession.sessionState.functionRegistry

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformAllExpressions {
      case f: UnresolvedFunction if !functionRegistry.functionExists(f.name) =>
        throw new NoSuchFunctionException("", f.name)
    }
}

/** Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
  */
object ResolveRelations extends Rule[LogicalPlan] {

  def catalog = SeccoSession.currentSession.sessionState.catalog

  def resolveRelation(plan: UnresolvedRelation): LogicalPlan =
    plan match {
      case u: UnresolvedRelation =>
        val relation = catalog.getTable(u.tableName)
        relation
          .map(f => Relation(TableIdentifier(f.tableName)))
          .getOrElse(throw new NoSuchTableException("", s"${u.tableName}"))
      case _ => plan
    }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperators { case u: UnresolvedRelation =>
      resolveRelation(u)
    }
}

/** Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
  * a logical plan node's children. And, it expands the star.
  */
object ResolveReferences extends Rule[LogicalPlan] {

  def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.map {
      case a: Alias => Alias(a.child, a.name)()
      case other    => other
    }
  }

  def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
    AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
  }

  /** Generate a new logical plan for the right child with different expression IDs
    * for all conflicting attributes.
    */
  private def dedupRight(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    val conflictingAttributes = left.outputSet.intersect(right.outputSet)
    logDebug(
      s"Conflicting attributes ${conflictingAttributes.mkString(",")} " +
        s"between $left and $right"
    )

    right.collect {
      // Handle base relations that might appear more than once.
      case oldVersion: MultiInstanceRelation
          if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
        val newVersion = oldVersion.newInstance()
        (oldVersion, newVersion)

      // Handle projects that create conflicting aliases.
      case oldVersion @ Project(_, projectList, _)
          if findAliases(projectList)
            .intersect(conflictingAttributes)
            .nonEmpty =>
        (oldVersion, oldVersion.copy(projectionList = newAliases(projectList)))

      case oldVersion @ Aggregate(_, aggregateExpressions, _, _)
          if findAliases(aggregateExpressions)
            .intersect(conflictingAttributes)
            .nonEmpty =>
        (
          oldVersion,
          oldVersion.copy(aggregateExpressions =
            newAliases(aggregateExpressions)
          )
        )
    }
    // Only handle first case, others will be fixed on the next pass.
      .headOption match {
      case None =>
        /*
         * No result implies that there is a logical plan node that produces new references
         * that this rule cannot handle. When that is the case, there must be another rule
         * that resolves these conflicts. Otherwise, the analysis will fail.
         */
        right
      case Some((oldRelation, newRelation)) =>
        rewritePlan(right, Map(oldRelation -> newRelation))._1
    }
  }

  private def rewritePlan(
      plan: LogicalPlan,
      conflictPlanMap: Map[LogicalPlan, LogicalPlan]
  ): (LogicalPlan, Seq[(Attribute, Attribute)]) = {
    if (conflictPlanMap.contains(plan)) {
      // If the plan is the one that conflict the with left one, we'd
      // just replace it with the new plan and collect the rewrite
      // attributes for the parent node.
      val newRelation = conflictPlanMap(plan)
      newRelation -> plan.output.zip(newRelation.output)
    } else {
      val attrMapping = new mutable.ArrayBuffer[(Attribute, Attribute)]()
      val newPlan = plan.mapChildren { child =>
        // If not, we'd rewrite child plan recursively until we find the
        // conflict node or reach the leaf node.
        val (newChild, childAttrMapping) = rewritePlan(child, conflictPlanMap)
        attrMapping ++= childAttrMapping.filter { case (oldAttr, _) =>
          // `attrMapping` is not only used to replace the attributes of the current `plan`,
          // but also to be propagated to the parent plans of the current `plan`. Therefore,
          // the `oldAttr` must be part of either `plan.references` (so that it can be used to
          // replace attributes of the current `plan`) or `plan.outputSet` (so that it can be
          // used by those parent plans).
          (plan.outputSet ++ plan.references).contains(oldAttr)
        }
        newChild
      }

      if (attrMapping.isEmpty) {
        newPlan -> attrMapping
      } else {
        assert(
          !attrMapping
            .groupBy(_._1.exprId)
            .exists(_._2.map(_._2.exprId).distinct.length > 1),
          "Found duplicate rewrite attributes"
        )
        val attributeRewrites = AttributeMap(attrMapping)
        // Using attrMapping from the children plans to rewrite their parent node.
        // Note that we shouldn't rewrite a node using attrMapping from its sibling nodes.
        newPlan.transformExpressions { case a: Attribute =>
          dedupAttr(a, attributeRewrites)
//          case s: SubqueryExpression =>
//            s.withNewPlan(
//              dedupOuterReferencesInSubquery(s.plan, attributeRewrites)
//            )
        } -> attrMapping
      }
    }
  }

  private def dedupAttr(
      attr: Attribute,
      attrMap: AttributeMap[Attribute]
  ): Attribute = {
    val exprId = attrMap.getOrElse(attr, attr).exprId
    attr.withExprId(exprId)
  }

  /** Returns true if `exprs` contains a [[Star]].
    */
  def containsStar(exprs: Seq[Expression]): Boolean =
    exprs.exists(_.collect { case _: Star => true }.nonEmpty)

  /** Expands the matching attribute.*'s in `child`'s output.
    */
  def expandStarExpression(
      expr: Expression,
      child: LogicalPlan
  ): Expression = {
    expr.transformUp {
      case f1: UnresolvedFunction if containsStar(f1.children) =>
        f1.copy(children = f1.children.flatMap {
          case s: Star => s.expand(child)
          case o       => o :: Nil
        })
      // count(*) has been replaced by count(1)
      case o if containsStar(o.children) =>
        throw new Exception(
          s"Invalid usage of '*' in expression '${o.prettyName}'"
        )
    }
  }

  /** Build a project list for Project/Aggregate and expand the star if possible
    */
  private def buildExpandedProjectList(
      exprs: Seq[NamedExpression],
      child: LogicalPlan
  ): Seq[NamedExpression] = {
    exprs
      .flatMap {
        // Using Dataframe/Dataset API: testData2.groupBy($"a", $"b").agg($"*")
        case s: Star => s.expand(child)
        // Using SQL API without running ResolveAlias: SELECT * FROM testData2 group by a, b
        case UnresolvedAlias(s: Star, _) => s.expand(child)
        case o if containsStar(o :: Nil) =>
          expandStarExpression(o, child) :: Nil
        case o => o :: Nil
      }
      .map(_.asInstanceOf[NamedExpression])
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperators {
      case p: LogicalPlan if !p.childrenResolved => p
      case p: Project if containsStar(p.projectionList) =>
        p.copy(projectionList =
          buildExpandedProjectList(p.projectionList, p.child)
        )
      case a: Aggregate if containsStar(a.aggregateExpressions) =>
        a.copy(aggregateExpressions =
          buildExpandedProjectList(a.aggregateExpressions, a.child)
        )
      case j @ BinaryJoin(left, right, _, _, _, _) if !j.duplicateResolved =>
        j.copy(right = dedupRight(left, right))
      case q: LogicalPlan =>
        logTrace(s"Attempting to resolve ${q.simpleString}")
        q.transformExpressionsUp { case u @ UnresolvedAttribute(nameParts) =>
          val result = q.resolveAttributeByChildren(nameParts).getOrElse(u)
          result
        }
    }
}

/** Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
  */
object ResolveFunctions extends Rule[LogicalPlan] {

  def functionRegistry =
    SeccoSession.currentSession.sessionState.functionRegistry

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperators { case q: LogicalPlan =>
      q transformExpressions {
        case u if !u.childrenResolved =>
          u // Skip until children are resolved
        case u @ UnresolvedFunction(name, children, isDistinct) =>
          functionRegistry.lookUpFunction(name, children)
      }
    }
}

/** Replaces [[UnresolvedAlias]]s with concrete aliases.
  */
object ResolveAliases extends Rule[LogicalPlan] {

  private def assignAliases(exprs: Seq[NamedExpression]) = {
    exprs.zipWithIndex
      .map { case (expr, i) =>
        expr.transformUp { case u @ UnresolvedAlias(child, optGenAliasFunc) =>
          child match {
            case ne: NamedExpression => ne
            case e if !e.resolved    => u
            case e if optGenAliasFunc.isDefined =>
              Alias(child, optGenAliasFunc.get.apply(e))()
            case e => Alias(e, e.sql)()
          }
        }
      }
      .asInstanceOf[Seq[NamedExpression]]
  }

  private def hasUnresolvedAlias(exprs: Seq[NamedExpression]) =
    exprs.exists(_.find(_.isInstanceOf[UnresolvedAlias]).isDefined)

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan resolveOperators {
      case a @ Aggregate(child, aggs, _, _)
          if child.resolved && hasUnresolvedAlias(aggs) =>
        a.copy(aggregateExpressions = assignAliases(aggs))
      case p @ Project(child, projectionList, _)
          if child.resolved && hasUnresolvedAlias(projectionList) =>
        p.copy(projectionList = assignAliases(projectionList))

    }
}

/** This rule resolves and rewrites subqueries inside expressions.
  *
  * Note: CTEs are handled in CTESubstitution.
  */
//TODO: implement this rule
object ResolveSubquery extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

/** Replace unresolved expressions in grouping keys with resolved ones in SELECT clauses.
  * This rule is expected to run after [[ResolveReferences]] applied.
  * This feature is optional.
  */
//TODO: implement this rule
object ResolveAggAliasInGroupBy extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

/** Removes natural or using joins by calculating output columns based on output from two sides,
  * Then apply a Project on a normal Join to eliminate natural or using join.
  */
object ResolveNaturalAndUsingJoin extends Rule[LogicalPlan] {

  def resolver: (String, String) => Boolean = { case (a, b) =>
    a.equalsIgnoreCase(b)
  }

  private def commonNaturalJoinProcessing(
      left: LogicalPlan,
      right: LogicalPlan,
      joinType: JoinType,
      joinNames: Seq[String],
      condition: Option[Expression]
  ) = {
    val leftKeys = joinNames.map { keyName =>
      left.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(
          s"USING column `$keyName` cannot be resolved on the left " +
            s"side of the join. The left-side columns: [${left.output.map(_.name).mkString(", ")}]"
        )
      }
    }
    val rightKeys = joinNames.map { keyName =>
      right.output.find(attr => resolver(attr.name, keyName)).getOrElse {
        throw new AnalysisException(
          s"USING column `$keyName` cannot be resolved on the right " +
            s"side of the join. The right-side columns: [${right.output.map(_.name).mkString(", ")}]"
        )
      }
    }
    val joinPairs = leftKeys.zip(rightKeys)

    val newCondition =
      (condition ++ joinPairs.map(EqualTo.tupled)).reduceOption(And)

    // columns not in joinPairs
    val lUniqueOutput = left.output.filterNot(att => leftKeys.contains(att))
    val rUniqueOutput = right.output.filterNot(att => rightKeys.contains(att))

    // the output list looks like: join keys, columns from left, columns from right
    val projectList = joinType match {
      case LeftOuter =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput.map(_.withNullability(true))
      case LeftExistence(_) =>
        leftKeys ++ lUniqueOutput
      case RightOuter =>
        rightKeys ++ lUniqueOutput.map(_.withNullability(true)) ++ rUniqueOutput
      case FullOuter =>
        // in full outer join, joinCols should be non-null if there is.
        val joinedCols = joinPairs.map { case (l, r) =>
          Alias(Coalesce(Seq(l, r)), l.name)()
        }
        joinedCols ++
          lUniqueOutput.map(_.withNullability(true)) ++
          rUniqueOutput.map(_.withNullability(true))
      case _: InnerLike =>
        leftKeys ++ lUniqueOutput ++ rUniqueOutput
      case _ =>
        sys.error("Unsupported natural join type " + joinType)
    }
    // use Project to trim unnecessary fields
    Project(BinaryJoin(left, right, joinType, newCondition), projectList)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case j @ BinaryJoin(left, right, UsingJoin(joinType, usingCols), _, _, _)
        if left.resolved && right.resolved && j.duplicateResolved =>
      commonNaturalJoinProcessing(left, right, joinType, usingCols, None)
    case j @ BinaryJoin(left, right, NaturalJoin(joinType), condition, _, _)
        if j.resolvedExceptNatural =>
      // find common column names from both sides
      val joinNames =
        left.output.map(_.name).intersect(right.output.map(_.name))
      commonNaturalJoinProcessing(left, right, joinType, joinNames, condition)
  }
}

/** Turns projections that contain aggregate expressions into aggregations.
  */
object ResolveGlobalAggregatesInSelect extends Rule[LogicalPlan] {

  def containsAggregates(exprs: Seq[Expression]): Boolean = {
    // Find the first Aggregate Expression.
    exprs.exists(_.collectFirst { case ae: AggregateFunction =>
      ae
    }.isDefined)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperators {
      case Project(child, projectionList, mode)
          if containsAggregates(projectionList) =>
        Aggregate(
          child,
          projectionList,
          mode = mode
        )
    }
}

/** This rule finds aggregate expressions that are not in an aggregate operator.  For example,
  * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
  * underlying aggregate operator and then projected away after the original operator.
  */
//TODO: implement this rule
object ResolveAggregateFunctionsInHaving extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
