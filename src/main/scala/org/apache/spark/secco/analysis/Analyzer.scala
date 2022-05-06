package org.apache.spark.secco.analysis

import org.apache.spark.secco.analysis.rules.{
  CleanupAliases,
  ResolveAggAliasInGroupBy,
  ResolveAggregateFunctionsInHaving,
  ResolveAliases,
  ResolveFunctions,
  ResolveGlobalAggregatesInSelect,
  ResolveNaturalAndUsingJoin,
  ResolveReferences,
  ResolveRelations,
  ResolveSubquery
}
import org.apache.spark.secco.catalog.{Catalog, FunctionRegistry}
import org.apache.spark.secco.config.SeccoConfiguration
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.trees.RuleExecutor

/** A trivial [[Analyzer]] Used for testing when all relations are already filled in
  * and the analyzer needs only to resolve attribute references.
  */
object SimpleAnalyzer
    extends Analyzer(
      Catalog.defaultCatalog,
      SeccoConfiguration.newDefaultConf(),
      FunctionRegistry.newBuiltin
    )

/** Provides a way to keep state during the analysis, this enables us to decouple the concerns
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

/** Provides a logical query plan analyzer, which translates [[UnresolvedAttribute]]s and
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

    val resolutionRules = Seq(
      Batch(
        "Resolution",
        fixedPoint,
        ResolveRelations,
        ResolveReferences,
        ResolveFunctions,
        ResolveAliases,
        ResolveGlobalAggregatesInSelect,
        ResolveSubquery,
        ResolveAggAliasInGroupBy,
        ResolveNaturalAndUsingJoin,
        ResolveAggregateFunctionsInHaving
      )
    )

    val cleanUpRules = Seq(Batch("Cleanup", fixedPoint, CleanupAliases))

    resolutionRules ++ cleanUpRules
  }

}
