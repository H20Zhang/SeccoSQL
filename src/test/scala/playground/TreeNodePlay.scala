//package playground
//
//import org.apache.spark.gaga.deprecated.catalyst.tree.{
//  Rule,
//  RuleExecutor,
//  TreeNode
//}
//import org.scalatest.FunSuite
//
//abstract class LogicalPlan extends TreeNode[LogicalPlan] {}
//
//case class UnaryPlan(x: String) extends LogicalPlan {
//
//  /**
//    * Returns a Seq of the children of this node.
//    * Children should not change. Immutability required for containsChild optimization
//    */
//  override def children = Seq.empty[UnaryPlan]
//
//  /** ONE line description of this node with more information */
//  override def verboseString: String = s"$x"
//}
//
//case class BinaryPlan(x: String, lChild: LogicalPlan, rChild: LogicalPlan)
//    extends LogicalPlan {
//
//  /**
//    * Returns a Seq of the children of this node.
//    * Children should not change. Immutability required for containsChild optimization
//    */
//  override def children: Seq[LogicalPlan] = Seq(lChild, rChild)
//
//  /** ONE line description of this node with more information */
//  override def verboseString: String = s"$x"
//}
//
//case class AddRule() extends Rule[LogicalPlan] {
//
//  def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case node @ BinaryPlan("+", lChild, rChild) => {
//        lChild match {
//          case UnaryPlan(x) =>
//            rChild match {
//              case UnaryPlan(y) => UnaryPlan(s"${x.toInt + y.toInt}")
//              case _            => node
//            }
//          case _ => node
//        }
//      }
//    }
//}
//
//case class MultiRule() extends Rule[LogicalPlan] {
//
//  def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform {
//      case node @ BinaryPlan("*", lChild, rChild) => {
//        lChild match {
//          case UnaryPlan(x) =>
//            rChild match {
//              case UnaryPlan(y) => UnaryPlan(s"${x.toInt * y.toInt}")
//              case _            => node
//            }
//          case _ => node
//        }
//      }
//    }
//}
//
//class AddAndMultiRuleExecutor extends RuleExecutor[LogicalPlan] {
//
//  /** Defines a sequence of rule batches, to be overridden by the implementation. */
//  override protected def batches: Seq[Batch] =
//    Seq(Batch("AddAndMulti", FixedPoint(10), AddRule(), MultiRule()))
//
//}
//
//class TreeNodePlay extends FunSuite {
//
//  test("case") {
//    val a = List(1, 2, 3, 4)
//    val pf: PartialFunction[Int, String] = {
//      case 1 => "one"
//      case 2 => "2"
//      case 3 => "T"
//    }
//    val b = a.collect(pf)
//
//    println(b)
//  }
//
//  test("treenode") {
//
//    val node1 = UnaryPlan("1")
//    val node2 = UnaryPlan("2")
//    val node3 = UnaryPlan("3")
//    val node4 = BinaryPlan("+", node1, node2)
//    val node5 = BinaryPlan("*", node3, node4)
//
//    val node6 = node5 transform {
//      case node @ BinaryPlan("+", lChild, rChild) => {
//        lChild match {
//          case UnaryPlan(x) =>
//            rChild match {
//              case UnaryPlan(y) => UnaryPlan(s"${x.toInt + y.toInt}")
//              case _            => node
//            }
//          case _ => node
//        }
//      }
//      case node @ BinaryPlan("*", lChild, rChild) => {
//        lChild match {
//          case UnaryPlan(x) =>
//            rChild match {
//              case UnaryPlan(y) => UnaryPlan(s"${x.toInt * y.toInt}")
//              case _            => node
//            }
//          case _ => node
//        }
//      }
//    }
//
//    val ruleExecutor = new AddAndMultiRuleExecutor
//    val node7 = ruleExecutor.execute(node5)
//
////    val node6 = node5 transformUp {
////      case node @ BinaryPlan("+", lChild, rChild) => {
//////        UnaryPlan("+")
////        BinaryPlan("-", lChild, rChild)
////      }
////    }
//
////    println(node5.treeString)
////    println(node5.treeString(true, true))
//    println(node6.numberedTreeString)
//    println(node7.numberedTreeString)
//
//  }
//}
