import org.scalatest.Tag

package object util {

  object TestQuery extends Enumeration {
    type TestQuery = Value
    val simpleAcyclicJoin, simpleCyclicJoin, complexCyclicJoin1,
        complexCyclicJoin2, complexCyclicJoin3, joinWithCounting1,
        joinWithCounting2, joinWithPKFK1, joinWithPKFK2,
        joinWithAggregateAndProject1, joinWithAggregateAndProject2,
        joinWithAggregateAndProject3, joinWithAggregateAndProject4,
        simpleGraphAnalytic1, simpleGraphAnalytic2, simpleGraphAnalytic3,
        complexGraphAnalytic1, complexGraphAnalytic2 =
      Value
  }

  object IntegrationTestTag extends Tag("integration")

  object UnitTestTag extends Tag("unit")
}
