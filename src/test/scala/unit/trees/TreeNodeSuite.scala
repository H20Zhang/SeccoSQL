package unit.trees

import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.catalog.{CatalogColumn, CatalogTable}
import org.apache.spark.secco.execution.SharedContext
import org.apache.spark.secco.execution.plan.communication.{
  ShareConstraint,
  ShareConstraintContext
}
import org.apache.spark.secco.expression.utils.{AttributeMap, AttributeSet}
import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}
import org.apache.spark.secco.optimization.ExecMode.ExecMode
import org.apache.spark.secco.optimization.rules.{
  PushSelectionThroughJoin,
  RemoveRedundantSelection
}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.types.DataTypes
import util.{SeccoFunSuite, UnitTestTag}

case class PrintTestTreeNode(
    option: Attribute,
    mode: ExecMode
) extends LogicalPlan {

  /** The output attributes */
  override def output: Seq[Attribute] = option :: Nil

  override def children: Seq[LogicalPlan] = Nil
}

case class SharedContextTreeNode(
    child: LogicalPlan,
    option: Attribute,
    sharedContext: SharedContext[Seq[Int]],
    mode: ExecMode
) extends LogicalPlan {

  /** The output attributes */
  override def output: Seq[Attribute] = option :: Nil

  override def children: Seq[LogicalPlan] = Seq(child)
}

class TreeNodeSuite extends SeccoFunSuite {

  test("basic", UnitTestTag) {

    //print related
    val testTreeNode = PrintTestTreeNode(
      AttributeReference("A", DataTypes.DoubleType)(),
      ExecMode.Coupled
    )

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

  test("sharedContext") {
    val sharedContext = SharedContext(Seq(1))

    val node1 = PrintTestTreeNode(
      AttributeReference("A", DataTypes.DoubleType)(),
      ExecMode.Coupled
    )

    val node2 = SharedContextTreeNode(
      node1,
      AttributeReference("B", DataTypes.DoubleType)(),
      sharedContext,
      ExecMode.Coupled
    )

    val node3 = SharedContextTreeNode(
      node2,
      AttributeReference("C", DataTypes.DoubleType)(),
      sharedContext,
      ExecMode.Coupled
    )

    val plan = node3 transform { case t: SharedContextTreeNode =>
      t.copy(option = AttributeReference("X", DataTypes.DoubleType)())
    }

    println(node3)
    println(plan)
  }

}
