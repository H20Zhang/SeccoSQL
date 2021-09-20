package org.apache.spark.secco.optimization

import org.apache.spark.secco.trees.TreeNode
import org.apache.spark.secco.util.misc.LogAble

abstract class Rule[TreeType <: TreeNode[_]] extends LogAble {

  /** Name for this rule, automatically inferred based on class name. */
  val ruleName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def apply(plan: TreeType): TreeType
}
