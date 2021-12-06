package org.apache.spark.secco.optimization.rules

import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan, Rule}
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.support.AnalyzeOutputSupport

/*---------------------------------------------------------------------------------------------------------------------
 *  This file contains rules for optimizing iterative computations.
 *
 *  0. AddCache: add cache for logical operators during iterative computations.
 *  1. RemoveRedundantCache: remove redundant cache.
 *---------------------------------------------------------------------------------------------------------------------
 */

/** A rule that adds the cache for a logical plan if its output size is small and it is static */
//object AddCache extends Rule[LogicalPlan] with AnalyzeOutputSupport {
//
//  override def apply(plan: LogicalPlan): LogicalPlan =
//    plan transform { case i: Iterative =>
//      val updatePlans = i.collect { case u: Update =>
//        u
//      }
//
//      val assignPlans = i.collect { case a: Assign =>
//        a
//      }
//
//      val deltaTableIdentifiers = updatePlans.map(
//        _.deltaTableIdentifier
//      ) ++ assignPlans.map(_.tableIdentifier)
//
//      val newChild = i.child transformUp { case l: LogicalPlan =>
//        if (l.mode != ExecMode.Atomic && isStatic(l, deltaTableIdentifiers)) {
//
//          if (isMaterializable(l)) {
//            Cache(l)
//          } else {
//            l
//          }
//        } else {
//          l
//        }
//      }
//
//      i.copy(child = newChild)
//    }
//}

/** A rule that removes the redundant cache,
  * which means we do not cache a logical plan if its ancestor has already been cached
  */
object RemoveRedundantCache
    extends Rule[LogicalPlan]
    with AnalyzeOutputSupport {
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transform { case c: Cache =>
      val newChild = c.child transform { case ca: Cache =>
        ca.child
      }
      c.copy(child = newChild)
    }
}
