package util

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.{Catalog, CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.plan.Relation
import org.apache.spark.secco.execution.InternalRow
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.rdd.RDD
import util.TestQuery.TestQuery

/** TestCase provides test-case queries to validate the correctness
  * of the optimizer and planner.
  */
object TestCase {

  import org.apache.spark.secco.analysis.RelationAlgebraWithAnalysis._

  val cardinality = 10
  val valueUpperBound = 20

  private def genData(plan: LogicalPlan) = {
    TestDataGenerator.genRandomData(plan, cardinality, valueUpperBound)
    plan
  }

  def assignArrayData(
      plan: LogicalPlan,
      arrayOfScans: Map[String, Array[InternalRow]]
  ) = {
    val scans = plan.collect {
      case s: Relation => s
    }

    scans.foreach { scan =>
      TestDataGenerator.assignDataForScan(scan, arrayOfScans(scan.tableName))
    }

    plan
  }

  def assignRDDData(
      plan: LogicalPlan,
      rddOfScans: Map[String, RDD[InternalRow]]
  ) = {
    val scans = plan.collect {
      case s: Relation => s
    }

    scans.foreach { scan =>
      TestDataGenerator.assignDataForScan(scan, rddOfScans(scan.tableName))
    }

    plan
  }

  private def getPlan(testQuery: TestQuery) =
    testQuery match {
      case util.TestQuery.simpleAcyclicJoin  => simpleAcyclicJoinQuery
      case util.TestQuery.simpleCyclicJoin   => simpleCyclicJoinQuery
      case util.TestQuery.complexCyclicJoin1 => complexCyclicJoin1Query
      case util.TestQuery.complexCyclicJoin2 => complexCyclicJoin2Query
      case util.TestQuery.complexCyclicJoin3 => complexCyclicJoin3Query
      case util.TestQuery.joinWithCounting1  => joinWithCounting1Query
      case util.TestQuery.joinWithCounting2  => joinWithCounting2Query
      case util.TestQuery.joinWithPKFK1      => joinWithPKFK1Query
      case util.TestQuery.joinWithPKFK2      => joinWithPKFK2Query
      case util.TestQuery.joinWithAggregateAndProject1 =>
        joinWithAggregateAndProject1Query
      case util.TestQuery.joinWithAggregateAndProject2 =>
        joinWithAggregateAndProject2Query
      case util.TestQuery.joinWithAggregateAndProject3 =>
        joinWithAggregateAndProject3Query
      case util.TestQuery.joinWithAggregateAndProject4 =>
        joinWithAggregateAndProject4Query
      case util.TestQuery.simpleGraphAnalytic1  => simpleGraphAnalytic1Query
      case util.TestQuery.simpleGraphAnalytic2  => simpleGraphAnalytic2Query
      case util.TestQuery.simpleGraphAnalytic3  => simpleGraphAnalytic3Query
      case util.TestQuery.complexGraphAnalytic1 => complexGraphAnalytic1Query
      case util.TestQuery.complexGraphAnalytic2 => complexGraphAnalytic2Query
      case _ =>
        throw new Exception(s"no such test query:${testQuery} defined in $this")
    }

  def queryWithArrayInput(
      testQuery: TestQuery,
      dataOfScans: Map[String, Array[InternalRow]] = Map()
  ) = {
    val plan = getPlan(testQuery)

    dataOfScans match {
      case data if data.isEmpty => genData(plan)
      case arrayOfScans: Map[String, Array[InternalRow]] =>
        assignArrayData(plan, arrayOfScans)
      case _ =>
        throw new Exception(
          s"type of dataOfScans:${dataOfScans.getClass} is not supported"
        )
    }
  }

  def queryWithRDDInput(
      testQuery: TestQuery,
      dataOfScans: Map[String, RDD[InternalRow]] = Map()
  ) = {
    val plan = getPlan(testQuery)

    dataOfScans match {
      case data if data.isEmpty => genData(plan)
      case rddOfScans: Map[String, RDD[InternalRow]] =>
        assignRDDData(plan, rddOfScans)
      case _ =>
        throw new Exception(
          s"type of dataOfScans:${dataOfScans.getClass} is not supported"
        )
    }
  }

  /** Simple Acyclic Join
    *
    * Join
    *   R1(A,B)
    *   R2(B,C)
    */
  lazy val simpleAcyclicJoinQuery = {
    join("R1(A, B)", "R2(B, C)")
  }

  /** Simple Cyclic Join
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(A, C)
    */
  lazy val simpleCyclicJoinQuery = join("R1(A, B)", "R2(B, C)", "R3(A, C)")

  /** Complex Cyclic Join1
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(C, D)
    *   R4(D, A)
    */
  lazy val complexCyclicJoin1Query =
    join("R1(A, B)", "R2(B, C)", "R3(C, D)", "R4(D, A)")

  /** Complex Cyclic Join2
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(C, D)
    *   R4(D, E)
    *   R5(A, E)
    *   R6(A, C)
    *   R7(A, D)
    */
  lazy val complexCyclicJoin2Query = join(
    "R1(A, B)",
    "R2(B, C)",
    "R3(C, D)",
    "R4(D, E)",
    "R5(A, E)",
    "R6(A, C)",
    "R7(A, D)"
  )

  /** Complex Cyclic Join3
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(C, D)
    *   R4(D, E)
    *   R5(A, E)
    *   R6(C, E)
    */
  lazy val complexCyclicJoin3Query = join(
    "R1(A, B)",
    "R2(B, C)",
    "R3(C, D)",
    "R4(D, E)",
    "R5(A, E)",
    "R6(C, E)"
  )

  /** Join With Counting1
    *
    * Aggregate(B, Count(*))
    *   Join
    *     R1(A, B)
    *     R2(B, C)
    */
  lazy val joinWithCounting1Query =
    aggregate("count(*) by B", join("R1(A, B)", "R2(B, C)"))

  /** Join With Counting2
    *
    * Aggregate(A, Count(*))
    *   Join
    *     R1(A, B)
    *     R2(B, C)
    *     R3(C, D)
    *     R4(D, E)
    *     R5(A, E)
    *     R6(C, E)
    */
  lazy val joinWithCounting2Query = aggregate(
    "count(*) by A",
    join(
      "R1(A, B)",
      "R2(B, C)",
      "R3(C, D)",
      "R4(D, E)",
      "R5(A, E)",
      "R6(C, E)"
    )
  )

  /** Join Containing PKFKJoin1
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(C, D) Key:C
    */
  lazy val joinWithPKFK1Query = join("R1(A, B)", "R2(B, C)", "R3([C], C, D)")

  /** Join Containing PKFKJoin2
    *
    * Join
    *   R1(A, B)
    *   R2(B, C)
    *   R3(C, D)
    *   R4(D, A)
    *   R5(D, E) Key:D
    */
  lazy val joinWithPKFK2Query =
    join("R1(A, B)", "R2(B, C)", "R3(C, D)", "R4(D, A)", "R5([D], D, E)")

  /** Join With Aggregate and Project1
    *
    * Aggregate(A, Count(*))
    *   Project(A, C)
    *     Join
    *       R1(A, B)
    *       R2(B, C)
    *       R3(C, D)
    *       R4(D, A)
    */
  lazy val joinWithAggregateAndProject1Query =
    aggregate(
      "count(*) by A",
      project("A, C", join("R1(A, B)", "R2(B, C)", "R3(C, D)", "R4(D, A)"))
    )

  /** Join With Aggregate and Project2
    *
    * Aggregate(B, Count(*))
    *   Project(B, D)
    *     Join
    *       R1(A, B)
    *       R2(B, C)
    *       R3(C, D)
    *       R4(D, A)
    *       R5(A, C)
    */
  lazy val joinWithAggregateAndProject2Query =
    aggregate(
      "count(*) by B",
      project(
        "B, D",
        join("R1(A, B)", "R2(B, C)", "R3(C, D)", "R4(D, A)", "R5(A, C)")
      )
    )

  /** Join With Aggregate and Project3
    *
    * Aggregate(B, Count(*))
    *   Project(B, C)
    *     Join
    *       R1(A, B)
    *       R2(B, C)
    *       R3(A, C)
    *       R4(C, D)
    */
  lazy val joinWithAggregateAndProject3Query =
    aggregate(
      "count(*) by B",
      project("B, C", join("R1(A, B)", "R2(B, C)", "R3(A, C)", "R4(C, D)"))
    )

  /** Join With Aggregate and Project4
    *
    * Aggregate(A, sum(w))
    *   Join
    *     Project(A, C)
    *       Join
    *         R1(A, B)
    *         R2(B, C)
    *     R3(C, W) key: C
    */
  lazy val joinWithAggregateAndProject4Query =
    aggregate(
      "sum(W) by A",
      join(project("A, C", join("R1(A, B)", "R2(B, C)")), "R3([C], C, W)")
    )

  /** Simple Graph Analytic --- PageRank
    *
    * Iterative(1)
    *   Update(W, Delta_W)
    *     Rename(src, weight)
    *     Transform(src, 0.85*weight3+0.15)
    *       Rename(src, weight3)
    *         Aggregate(src, Sum(weight2))
    *           Join
    *             G(src, dst)
    *             Rename(dst, weight2)
    *               Transform(dst, weight1*degree)
    *                 Join
    *                   DeltaW(dst, weight1)
    *                   Degree(dst, degree)
    */
  //fixme: the last update may be redundant
  lazy val simpleGraphAnalytic1Query = {
    SeccoSession.currentSession.sessionState.catalog
      .createTable(CatalogTable("W", "src", "weight"))

    val expr = iterative(
      update(
        "W",
        "DeltaW",
        Seq("src"),
        rename(
          "src, weight",
          transform(
            "src, 0.85*weight3+0.15",
            rename(
              "src, weight3",
              aggregate(
                "sum(weight2) by src",
                join(
                  "G(src, dst)",
                  rename(
                    "dst, weight2",
                    transform(
                      "dst, weight1*degree",
                      join(
                        "DeltaW([dst], dst, weight1)",
                        "Degree([dst], dst, degree)"
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ),
      "W",
      1
    )
    expr
  }

  /** Simple Graph Analytic --- Connected Components
    *
    * Iterative(1)
    *   Update(W, Delta_W)
    *     Rename(src, weight)
    *       Aggregate(src, min(weight))
    *         Join
    *           G(src, dst)
    *           DeltaW(dst, weight)
    */
  lazy val simpleGraphAnalytic2Query = {
    SeccoSession.currentSession.sessionState.catalog
      .createTable(CatalogTable("W", "src", "weight"))

    val expr = iterative(
      update(
        "W",
        "DeltaW",
        Seq("src"),
        rename(
          "src, weight",
          aggregate(
            "min(weight) by src",
            join("G(src, dst)", "DeltaW([dst], dst, weight)")
          )
        )
      ),
      "W",
      1
    )
    expr
  }

  /** Simple Graph Analytic --- Single Source Shortest Path
    *
    * Iterative(20)
    *   Update(W, Delta_W)
    *     Rename(src, weight)
    *       Aggregate(src, min(weight2))
    *         Join
    *           G(src, dst)
    *           Rename(dst, weight2)
    *             Transform(dst, weight+1.0)
    *               Delta_W(dst, weight)
    */
  //fixme: this algebra cannot correctly compute the shortest path.
  lazy val simpleGraphAnalytic3Query = {
    SeccoSession.currentSession.sessionState.catalog
      .createTable(CatalogTable("W", "src", "weight"))

    val expr = iterative(
      update(
        "W",
        "DeltaW",
        Seq("src"),
        rename(
          "src, weight",
          aggregate(
            "min(weight2) by src",
            join(
              "G(src, dst)",
              rename(
                "dst, weight2",
                transform("dst, weight+1.0", "DeltaW([dst], dst, weight)")
              )
            )
          )
        )
      ),
      "W"
    )
    expr
  }

  /** Complex Graph Analytic1 --- PageRank
    *
    * Iterative(1)
    *   Update(W, Delta_W)
    *     Rename(src, weight)
    *     Transform(src, 0.85*weight3+0.15)
    *       Rename(src, weight3)
    *         Aggregate(src, Sum(weight2))
    *           Join
    *             Rename(src/A, dst/C)
    *               Join
    *                 R1(A, B)
    *                 R2(B, C)
    *             Rename(dst, weight2)
    *               Transform(dst, weight1*degree)
    *                 Join
    *                   DeltaW(dst, weight1)
    *                   Degree(dst, degree)
    */
  lazy val complexGraphAnalytic1Query = {
    SeccoSession.currentSession.sessionState.catalog
      .createTable(CatalogTable("W", "src", "weight"))

    val expr = iterative(
      update(
        "W",
        "DeltaW",
        Seq("src"),
        rename(
          "src, weight",
          transform(
            "src, 0.85*weight3+0.15",
            rename(
              "src, weight3",
              aggregate(
                "sum(weight2) by src",
                join(
                  rename("src/A, dst/C", join("R1(A, B)", "R2(B, C)")),
                  rename(
                    "dst, weight2",
                    transform(
                      "dst, weight1*degree",
                      join(
                        "DeltaW([dst], dst, weight1)",
                        "Degree([dst], dst, degree)"
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ),
      "W",
      1
    )
    expr
  }

  /** Complex Graph Analytic2 --- PageRank
    *
    * Iterative(1)
    *   Update(W, Delta_W)
    *     Rename(src, weight)
    *     Transform(src, 0.85*weight3+0.15)
    *       Rename(src, weight3)
    *         Aggregate(src, Sum(weight2))
    *           Join
    *             Rename(src/A, dst/C)
    *               Join
    *                 R1(A, B)
    *                 R2(B, C)
    *             Rename(dst, weight2)
    *               Transform(dst, weight1*degree)
    *                 Join
    *                   DeltaW(dst, weight1)
    *                   Degree(dst, degree)
    */
  lazy val complexGraphAnalytic2Query = {
    SeccoSession.currentSession.sessionState.catalog
      .createTable(CatalogTable("W", "src", "weight"))

    val expr = iterative(
      update(
        "W",
        "DeltaW",
        Seq("src"),
        rename(
          "src, weight",
          transform(
            "src, 0.85*weight3+0.15",
            rename(
              "src, weight3",
              aggregate(
                "sum(weight2) by src",
                join(
                  rename(
                    "src/A, dst/C",
                    project("A, C", join("R1(A, B)", "R2(B, C)"))
                  ),
                  rename(
                    "dst, weight2",
                    transform(
                      "dst, weight1*degree",
                      join(
                        "DeltaW([dst], dst, weight1)",
                        "Degree([dst], dst, degree)"
                      )
                    )
                  )
                )
              )
            )
          )
        )
      ),
      "W",
      1
    )
    expr
  }

}
