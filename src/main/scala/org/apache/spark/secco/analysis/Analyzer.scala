package org.apache.spark.secco.analysis

import org.apache.spark.secco.catalog.{Catalog, FunctionRegistry}
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.expression.aggregate.{
  AggregateFunction,
  Count,
  Max,
  Min,
  Sum
}
import org.apache.spark.secco.expression.{
  Alias,
  Expression,
  NamedExpression,
  Star
}
import org.apache.spark.secco.optimization.{LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan.{
  Aggregate,
  Distinct,
  Join,
  JoinType,
  Project,
  Relation
}
import org.apache.spark.secco.trees.RuleExecutor

/**
  * A trivial [[Analyzer]] Used for testing when all relations are already filled in
  * and the analyzer needs only to resolve attribute references.
  */
object SimpleAnalyzer
    extends Analyzer(
      Catalog.defaultCatalog,
      SeccoConfiguration.newDefaultConf(),
      FunctionRegistry.newBuiltin
    )

/**
  * Provides a way to keep state during the analysis, this enables us to decouple the concerns
  * of analysis environment from the catalog.
  *
  * Note this is thread local.
  *
  * @param defaultDatabase The default database used in the view resolution, this overrules the
  *                        current catalog database.
  * @param nestedViewDepth The nested depth in the view resolution, this enables us to limit the
  *                        depth of nested views.
  */
case class AnalysisContext(
    defaultDatabase: Option[String] = None,
    nestedViewDepth: Int = 0
)

object AnalysisContext {
  private val value = new ThreadLocal[AnalysisContext]() {
    override def initialValue: AnalysisContext = AnalysisContext()
  }

  def get: AnalysisContext = value.get()
  private def set(context: AnalysisContext): Unit = value.set(context)

  def withAnalysisContext[A](database: Option[String])(f: => A): A = {
    val originContext = value.get()
    val context = AnalysisContext(
      defaultDatabase = database,
      nestedViewDepth = originContext.nestedViewDepth + 1
    )
    set(context)
    try f
    finally { set(originContext) }
  }
}

/**
  * Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
  * [[UnresolvedRelation]]s into fully typed objects using information in a
  * [[SessionCatalog]] and a [[FunctionRegistry]].
  */
class Analyzer(
    catalog: Catalog = Catalog.defaultCatalog,
    conf: SeccoConfiguration = SeccoConfiguration.newDefaultConf(),
    functionRegistry: FunctionRegistry = FunctionRegistry.newBuiltin,
    maxIterations: Int
) extends RuleExecutor[LogicalPlan] {

  def this(
      catalog: Catalog,
      conf: SeccoConfiguration,
      functionRegistry: FunctionRegistry
  ) = {
    this(catalog, conf, functionRegistry, conf.maxIteration)
  }

  protected val fixedPoint = FixedPoint(maxIterations)

  lazy val batches: Seq[Batch] = {

    val compatibleRules = conf.enableCompatiblity match {
      case true =>
        Seq(
          Batch(
            "Backward Compatibility",
            Once,
            MakeCompatibleForAggregate,
            MakeCompatibleForProject
            //      ,MakeCompatibleForWith
          )
        )
      case false => Seq[Batch]()
    }

    val resolutionRules = Seq(
      Batch(
        "Resolution",
        fixedPoint,
        ResolveRelations,
        ResolveReferences,
        ResolveFunctions,
        ResolveAliases,
        ResolveGlobalAggregatesInSelect
        //      ResolveSubquery, //TODO: implement later
        //      ResolveAggAliasInGroupBy,
        //      ResolveNaturalJoin,
        //      ResolveAggregateFunctionsInHaving
      )
    )

    val cleanUpRules = Seq(Batch("Cleanup", fixedPoint, CleanupAliases))

    compatibleRules ++ resolutionRules ++ cleanUpRules
  }

  //===Ad-hoc rules for compatibility===

  object MakeCompatibleForWith extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  object MakeCompatibleForAggregate extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.transform {
        case a @ Aggregate(
              _,
              _,
              _,
              _,
              _,
              groupingExpressions,
              aggregateExpressions
            ) =>
          val attributeExtractRegex = "`(\\w*)`".r
          val plainTextGroupingList = groupingExpressions
            .map(_.sql)
            .map(f =>
              attributeExtractRegex.findFirstMatchIn(f).get.subgroups(0)
            )

          val semiRingExpr =
            aggregateExpressions.filterNot(f =>
              plainTextGroupingList.contains(f.name)
            )

          if (semiRingExpr.size != 1)
            throw new Exception(
              s"only allow 1 semiRing, current semiRingExpr:${semiRingExpr}"
            )
          val (sumOp, addExpr) = semiRingExpr.head match {
            case Alias(Count(x), _) => ("count", x.sql)
            case Alias(Sum(x), _)   => ("sum", x.sql)
            case Alias(Min(x), _)   => ("min", x.sql)
            case Alias(Max(x), _)   => ("max", x.sql)
          }

          //TODO: also need to consider the case of expr for iterative query
          val attrOpt = attributeExtractRegex.findFirstMatchIn(addExpr)

          if (attrOpt.isDefined) {
            val attr = attrOpt.get.subgroups(0)

            a.copy(
              groupingListOld = plainTextGroupingList,
              semiringListOld = (sumOp, attr),
              groupingExpressions = Seq(),
              aggregateExpressions = Seq()
            )
          } else {
            throw new Exception("not supported aggregate types")
          }
      }
  }

  object MakeCompatibleForProject extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.transform {
        case Distinct(p @ Project(_, _, _, projectionList), _) =>
          p.copy(
            projectionListOld = projectionList.map(_.name),
            projectionList = Seq()
          )
        case p @ Project(child, _, _, projectionList)
            if projectionList
              .map(_.name)
              .toSet == child.output.map(_.name).toSet =>
          //TODO: this part is only a temporary solution
          p.copy(
            projectionListOld = projectionList.map(_.name),
            projectionList = Seq()
          )
      }
  }

  //===Analysis Rules===
  /**
    * Checks whether a function identifier referenced by an [[UnresolvedFunction]] is defined in the
    * function registry. Note that this rule doesn't try to resolve the [[UnresolvedFunction]]. It
    * only performs simple existence check according to the function identifier to quickly identify
    * undefined functions without triggering relation resolution, which may incur potentially
    * expensive partition/schema discovery process in some cases.
    *
    * @see [[ResolveFunctions]]
    */
  object LookupFunctions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.transformAllExpressions {
        case f: UnresolvedFunction
            if !functionRegistry.functionExists(f.name) =>
          throw new NoSuchFunctionException("", f.name)
      }
  }

  /**
    * Replaces [[UnresolvedRelation]]s with concrete relations from the catalog.
    */
  object ResolveRelations extends Rule[LogicalPlan] {

    def resolveRelation(plan: UnresolvedRelation): LogicalPlan =
      plan match {
        case u: UnresolvedRelation =>
          val relation = catalog.getTable(u.tableName)
          relation
            .map(f => Relation(f.tableName))
            .getOrElse(throw new Exception(s"no such table:${u.tableName}"))
        case _ => plan
      }

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
        case u: UnresolvedRelation => resolveRelation(u)
      }
  }

  /**
    * Replaces [[UnresolvedAttribute]]s with concrete [[AttributeReference]]s from
    * a logical plan node's children.
    */
  object ResolveReferences extends Rule[LogicalPlan] {

    /**
      * Returns true if `exprs` contains a [[Star]].
      */
    def containsStar(exprs: Seq[Expression]): Boolean =
      exprs.exists(_.collect { case _: Star => true }.nonEmpty)

    /**
      * Expands the matching attribute.*'s in `child`'s output.
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

    /**
      * Build a project list for Project/Aggregate and expand the star if possible
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

    /**
      * Generate new logical plans for all conflicting attributes of
      * children with different expression IDs.
      */
    //currently, for each input Relation, even with same name, their output's
    //attributeReference will have different exprId, thus, there is no need to deduplicate.
    //However, it is possible that in the future, with more SQL feature added, deduplication
    //may become an necessity.
    def dedup(children: Seq[LogicalPlan]): Seq[LogicalPlan] = ???

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
//        case j @ Join(chilren, _, _, _) if !j.duplicatedResolved =>
//          j.copy(children = dedup(chilren))
        case q: LogicalPlan =>
          logTrace(s"Attempting to resolve ${q.simpleString}")
          q.transformExpressionsUp {
            case u @ UnresolvedAttribute(nameParts) =>
              val result = q.resolveAttributeByChildren(nameParts).getOrElse(u)
              result
          }
      }
  }

  /**
    * Replaces [[UnresolvedFunction]]s with concrete [[Expression]]s.
    */
  object ResolveFunctions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
        case q: LogicalPlan =>
          q transformExpressions {
            case u if !u.childrenResolved =>
              u // Skip until children are resolved
            case u @ UnresolvedFunction(name, children, isDistinct) =>
              functionRegistry.lookUpFunction(name, children)
          }
      }
  }

  /**
    * Replaces [[UnresolvedAlias]]s with concrete aliases.
    */
  object ResolveAliases extends Rule[LogicalPlan] {

    private def assignAliases(exprs: Seq[NamedExpression]) = {
      exprs.zipWithIndex
        .map {
          case (expr, i) =>
            expr.transformUp {
              case u @ UnresolvedAlias(child, optGenAliasFunc) =>
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
        case a @ Aggregate(child, _, _, _, _, _, aggs)
            if child.resolved && hasUnresolvedAlias(aggs) =>
          a.copy(aggregateExpressions = assignAliases(aggs))
        case p @ Project(child, _, _, projectionList)
            if child.resolved && hasUnresolvedAlias(projectionList) =>
          p.copy(projectionList = assignAliases(projectionList))

      }
  }

  /**
    * This rule resolves and rewrites subqueries inside expressions.
    *
    * Note: CTEs are handled in CTESubstitution.
    */
  object ResolveSubquery extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  /**
    * Replace unresolved expressions in grouping keys with resolved ones in SELECT clauses.
    * This rule is expected to run after [[ResolveReferences]] applied.
    * This feature is optional.
    */
  object ResolveAggAliasInGroupBy extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  /**
    * Removes natural join by calculating output columns based on output from two sides,
    * Then apply a Project on a normal Join to eliminate natural or using join.
    */
  object ResolveNaturalJoin extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  /**
    * Turns projections that contain aggregate expressions into aggregations.
    */
  object ResolveGlobalAggregatesInSelect extends Rule[LogicalPlan] {

    def containsAggregates(exprs: Seq[Expression]): Boolean = {
      // Find the first Aggregate Expression.
      exprs.exists(_.collectFirst {
        case ae: AggregateFunction => ae
      }.isDefined)
    }

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
        case Project(child, projectionListOld, mode, projectionList)
            if containsAggregates(projectionList) =>
          Aggregate(
            child,
            Seq(),
            (projectionListOld.toString(), "error"),
            Seq(),
            mode,
            Nil,
            projectionList
          )
      }
  }

  /**
    * This rule finds aggregate expressions that are not in an aggregate operator.  For example,
    * those in a HAVING clause or ORDER BY clause.  These expressions are pushed down to the
    * underlying aggregate operator and then projected away after the original operator.
    */
  object ResolveAggregateFunctionsInHaving extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  /**
    * The aggregate expressions from subquery referencing outer query block are pushed
    * down to the outer query block for evaluation. This rule below updates such outer references
    * as AttributeReference referring attributes from the parent/outer query block.
    *
    * For example (SQL):
    * {{{
    *   SELECT l.a FROM l GROUP BY 1 HAVING EXISTS (SELECT 1 FROM r WHERE r.d < min(l.b))
    * }}}
    * Plan before the rule.
    *    Project [a#226]
    *    +- Filter exists#245 [min(b#227)#249]
    *       :  +- Project [1 AS 1#247]
    *       :     +- Filter (d#238 < min(outer(b#227)))       <-----
    *       :        +- SubqueryAlias r
    *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
    *       :              +- LocalRelation [_1#234, _2#235]
    *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
    *          +- SubqueryAlias l
    *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
    *                +- LocalRelation [_1#223, _2#224]
    * Plan after the rule.
    *    Project [a#226]
    *    +- Filter exists#245 [min(b#227)#249]
    *       :  +- Project [1 AS 1#247]
    *       :     +- Filter (d#238 < outer(min(b#227)#249))   <-----
    *       :        +- SubqueryAlias r
    *       :           +- Project [_1#234 AS c#237, _2#235 AS d#238]
    *       :              +- LocalRelation [_1#234, _2#235]
    *       +- Aggregate [a#226], [a#226, min(b#227) AS min(b#227)#249]
    *          +- SubqueryAlias l
    *             +- Project [_1#223 AS a#226, _2#224 AS b#227]
    *                +- LocalRelation [_1#223, _2#224]
    */
  object UpdateOuterReferences extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = ???
  }

  /**
    * Cleans up unnecessary Aliases inside the plan. Basically we only need Alias as a top level
    * expression in Project(project list) or Aggregate(aggregate expressions) or
    * Window(window expressions).
    */
  object CleanupAliases extends Rule[LogicalPlan] {
    private def trimAliases(e: Expression): Expression = {
      e.transformDown {
        case Alias(child, _) => child
      }
    }

    def trimNonTopLevelAliases(e: Expression): Expression =
      e match {
        case a: Alias =>
          a.withNewChildren(trimAliases(a.child) :: Nil)
        case other => trimAliases(other)
      }

    override def apply(plan: LogicalPlan): LogicalPlan =
      plan.resolveOperators {
        case p @ Project(_, _, _, projectionList) =>
          val cleanedProjectList = {
            projectionList.map(
              trimNonTopLevelAliases(_).asInstanceOf[NamedExpression]
            )
          }
          p.copy(projectionList = cleanedProjectList)

        case a @ Aggregate(_, _, _, _, _, grouping, aggs) =>
          val cleanedAggs =
            aggs.map(trimNonTopLevelAliases(_).asInstanceOf[NamedExpression])
          a.copy(
            aggregateExpressions = cleanedAggs,
            groupingExpressions = grouping.map(trimAliases)
          )

        case other =>
          other transformExpressionsDown {
            case Alias(child, _) => child
          }
      }
  }

}
