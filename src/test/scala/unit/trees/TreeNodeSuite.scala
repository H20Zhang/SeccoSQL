package unit.trees

import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.rules.{
  PushSelectionThroughJoin,
  RemoveRedundantSelection
}
import org.apache.spark.secco.optimization.plan._
import util.{SeccoFunSuite, UnitTestTag}

class TreeNodeSuite extends SeccoFunSuite {

  case class PrintTestTreeNode(option: String, mode: ExecMode)
      extends LogicalPlan {

    /** The output attributes */
    override def outputOld: Seq[String] = option :: Nil

    override def children: Seq[LogicalPlan] = Nil
  }

  test("basic", UnitTestTag) {

    //print related
    val testTreeNode = PrintTestTreeNode("A", ExecMode.Coupled)

    assert(testTreeNode.treeString == "PrintTestTreeNode[A, Coupled]-> (A)\n")
    assert(testTreeNode.simpleString == "PrintTestTreeNode[A, Coupled]")
    assert(testTreeNode.verboseString == "PrintTestTreeNode[A, Coupled]-> (A)")
    assert(
      testTreeNode.verboseStringWithSuffix == "PrintTestTreeNode[A, Coupled]-> (A)"
    )
    assert(testTreeNode.nodeName == "PrintTestTreeNode")
    assert(testTreeNode.argString == "[A, Coupled]")

    //transform related

  }

}
