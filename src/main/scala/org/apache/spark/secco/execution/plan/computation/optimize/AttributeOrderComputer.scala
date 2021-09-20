package org.apache.spark.secco.execution.plan.computation.optimize

import org.apache.spark.secco.{SeccoSession, PreferenceLevel}
import org.apache.spark.secco.PreferenceLevel.PreferenceLevel
import org.apache.spark.secco.execution.SeccoPlan
import org.apache.spark.secco.optimization.plan.JoinType
import org.apache.spark.secco.execution.plan.computation._
import org.apache.spark.secco.execution.statsComputation.StatisticKeeper

import scala.collection.mutable

//TODO: DEBUG
class AttributeOrderComputer(
    rootPlan: LocalProcessingExec,
    sharedAttributeOrder: mutable.ArrayBuffer[String],
    placeHolderStatistics: Seq[StatisticKeeper]
) {

  private lazy val rootPlanWithBaseAndIndexDetermined = {
    val (_, baseAndIndexInBinaryJoinMap) =
      determinePreferenceAndBaseIndexInBinary(rootPlan)
    val localExec = rootPlan transform {
      case j @ LocalJoinExec(children, joinType, _)
          if (joinType == JoinType.GHDFKFK || joinType == JoinType.FKFK || joinType == JoinType.PKFK) =>
        j.copy(children =
          Seq(
            baseAndIndexInBinaryJoinMap(j)._1,
            baseAndIndexInBinaryJoinMap(j)._2
          )
        )
      case c @ LocalCartesianProductExec(children, _) =>
        c.copy(children =
          Seq(
            baseAndIndexInBinaryJoinMap(c)._1,
            baseAndIndexInBinaryJoinMap(c)._2
          )
        )
    }

    localExec.asInstanceOf[LocalProcessingExec]
  }

  /** priority of preferredOrder of each LocalExec is
    *   LocalPlaceHolderExec < LocalAggregateExec = LocalProjectExec < LocalJoinExec = LocalDiffExec < LocalCartesianProductExec
    */
  private def determinePreferenceAndBaseIndexInBinary(rootPlan: SeccoPlan) = {

//    println(s"[debug]: rootPlan:${rootPlan}")

    val conf = SeccoSession.currentSession.sessionState.conf
    // HashMap(ChildOp, (ParentOp, ParentOp's Preferred Order, isStrongPreference))
    val preferenceMap =
      mutable
        .HashMap[
          SeccoPlan,
          (Option[SeccoPlan], Set[String], PreferenceLevel)
        ]()
    val baseAndIndexInBinaryJoinMap =
      mutable.HashMap[SeccoPlan, (LocalProcessingExec, LocalProcessingExec)]()

    preferenceMap += (rootPlan -> (None, Set(), PreferenceLevel.MaybeSatisfy))

    var curLevelNodes = Seq[SeccoPlan](rootPlan)

    while (curLevelNodes.nonEmpty) {
      curLevelNodes = curLevelNodes.flatMap { plan =>
        val preference = preferenceMap(plan)

        plan match {
          case s: LocalSelectExec =>
            preferenceMap(s.child) = (Some(s), preference._2, preference._3)
            s.children
          case p: LocalProjectExec =>
            //mark preferred order of the chlidren
            if (preference._1.nonEmpty) {
              //p is not root
              preference._3 match {
                case PreferenceLevel.SatisfyAll =>
                  preferenceMap(p.child) =
                    (Some(p), preference._2, PreferenceLevel.SatisfyAll)
                case PreferenceLevel.SatisfyOne =>
                  assert(
                    preference._2.intersect(p.projectionList.toSet).nonEmpty
                  )

                  preferenceMap(p.child) = (
                    Some(p),
                    preference._2.intersect(p.projectionList.toSet),
                    PreferenceLevel.SatisfyOne
                  )
                case PreferenceLevel.MaybeSatisfy =>
                  preferenceMap(p.child) = (
                    Some(p),
                    p.projectionList.toSet,
                    PreferenceLevel.SatisfyOne
                  )
              }

            } else {
              //p is root
              preferenceMap(p.child) =
                (Some(p), p.projectionList.toSet, PreferenceLevel.MaybeSatisfy)
            }

            //add children to queue
            p.children
          case a: LocalSemiringAggregateExec =>
            //mark preferred order of the chlidren
            if (preference._1.nonEmpty) {
              //p is not root
              preference._3 match {
                case PreferenceLevel.SatisfyAll =>
                  preferenceMap(a.child) =
                    (Some(a), preference._2, PreferenceLevel.SatisfyAll)
                case PreferenceLevel.SatisfyOne =>
                  assert(
                    preference._2.intersect(a.groupingList.toSet).nonEmpty
                  )

                  preferenceMap(a.child) = (
                    Some(a),
                    preference._2.intersect(a.groupingList.toSet),
                    PreferenceLevel.SatisfyOne
                  )
                case PreferenceLevel.MaybeSatisfy =>
                  preferenceMap(a.child) = (
                    Some(a),
                    a.groupingList.toSet,
                    PreferenceLevel.SatisfyOne
                  )
              }

            } else {
              //p is root
              preferenceMap(a.child) =
                (Some(a), a.groupingList.toSet, PreferenceLevel.MaybeSatisfy)
            }

            //add children to queue
            a.children
//          case j @ LocalJoinExec(children, joinType, _)
//              if (joinType == JoinType.GHD) =>
//            j.children.foreach { child =>
//              preferenceMap(child) = (Some(j), preference._2, preference._3)
//            }
//
//            //fixme: there exists some bugs
//            j.children
          case j @ LocalJoinExec(children, joinType, _)
              if (joinType == JoinType.GHDFKFK || joinType == JoinType.FKFK || joinType == JoinType.PKFK) =>
            //mark preferred order of the chlidren
            var childrenToAdd: Seq[LocalProcessingExec] = null
            val preferredOrderOfParent = preference._2
            val parentPreferenceLevel = preference._3

            //find the one with largest overlap
            val baseChildPreference = children.zipWithIndex
              .map {
                case (p, pos) =>
                  //the more common attribute, the better
                  val commonAttributeWidthPriority =
                    preferredOrderOfParent.intersect(p.outputOld.toSet).size

                  // the more front, the better
                  val posPriority = -pos

//                  if (parentPreferenceLevel == PreferenceLevel.MaybeSatisfy) {

                  // the larger, the better
                  val opPriority = p match {
                    case p: LocalPlaceHolderExec                        => 1
                    case j: LocalJoinExec if j.joinType == JoinType.GHD => 1
                    // the binary join must be the baseChild, otherwise it hinders pipelining and result in failure,
                    // when join schema contains cycle
                    case j: LocalJoinExec
                        if j.joinType == JoinType.PKFK || j.joinType == JoinType.GHDFKFK || j.joinType == JoinType.FKFK =>
                      2
                    //for project and aggregate, it is better to use them as index, which ensures correctness.
                    case _ => 0
                  }

                  // if only decouple optimization is enabled, the position of the children determines order
                  if (conf.enableOnlyDecoupleOptimization == true) {
                    (
                      p,
                      posPriority,
                      opPriority,
                      commonAttributeWidthPriority
                    )
                  } else {
                    (
                      p,
                      opPriority,
                      commonAttributeWidthPriority,
                      posPriority
                    )
                  }

//                  } else {
//
//                    // the larger, the better
//                    val opPriority = p match {
//                      case p: LocalPlaceHolderExec => 2
//                      case j: LocalJoinExec        => 1
//                      case _                       => 0
//                    }
//
//                    (
//                      p,
//                      commonAttributeWidthPriority,
//                      opPriority,
//                      posPriority
//                    )
//                  }
              }
              .sortBy(f => (f._2, f._3, f._4))
              .reverse
            val baseChild = baseChildPreference.head._1
            val indexChild = children.diff(Seq(baseChild)).head
            childrenToAdd = baseChild :: indexChild :: Nil

//            println(s"[debug]:baseChild:${baseChild}")
//            println(s"[debug]:indexChild:${indexChild}")

            val joinKeySet =
              baseChild.outputOld.intersect(indexChild.outputOld).toSet

            preference._3 match {
              case PreferenceLevel.SatisfyAll =>
                assert(
                  preferredOrderOfParent.subsetOf(baseChild.outputOld.toSet),
                  s"Cannot satisfy conditions for PreferenceLevel.SatisfyAll as baseChild.output:${baseChild.outputOld} " +
                    s"does not contain all element in preferredOrderofParent:${preferredOrderOfParent}"
                )
                preferenceMap(baseChild) = (
                  Some(j),
                  preferredOrderOfParent.intersect(baseChild.outputOld.toSet),
                  PreferenceLevel.SatisfyAll
                )
              case PreferenceLevel.SatisfyOne =>
                preferenceMap(baseChild) = (
                  Some(j),
                  preferredOrderOfParent.intersect(baseChild.outputOld.toSet),
                  PreferenceLevel.SatisfyOne
                )
              case PreferenceLevel.MaybeSatisfy =>
                preferenceMap(baseChild) = (
                  Some(j),
                  preferredOrderOfParent.intersect(baseChild.outputOld.toSet),
                  PreferenceLevel.MaybeSatisfy
                )
            }

            preferenceMap(indexChild) =
              (Some(j), joinKeySet, PreferenceLevel.SatisfyAll)

            //add base and index information to help transform plan
            baseAndIndexInBinaryJoinMap(j) = (baseChild, indexChild)

            //add children to queue
            childrenToAdd

          case c @ LocalCartesianProductExec(children, _) =>
            //mark preferred order of the chlidren
            var childrenToAdd: Seq[LocalProcessingExec] = null
            val preferredOrderOfParent = preference._2
            val parentPreferenceLevel = preference._3

            //find the one with largest overlap
            val containedAtLeastOneChildren = children.zipWithIndex
              .map {
                case (p, pos) =>
                  //more common attribute the better
                  val commonAttributeWidthPriority =
                    preferredOrderOfParent.intersect(p.outputOld.toSet).size

                  //scan > join > else
                  val opPriority = p match {
                    case c: LocalCartesianProductExec => 3
                    case p: LocalPlaceHolderExec      => 2
                    case j: LocalJoinExec             => 1
                    case _                            => 0
                  }
                  val posPriority = -pos

                  // if only decouple optimization is enabled, the position of the children determines order
                  if (conf.enableOnlyDecoupleOptimization == true) {
                    (
                      p,
                      posPriority,
                      opPriority,
                      commonAttributeWidthPriority
                    )
                  } else if (
                    parentPreferenceLevel == PreferenceLevel.MaybeSatisfy
                  ) {
                    (
                      p,
                      opPriority,
                      commonAttributeWidthPriority,
                      posPriority
                    )
                  } else {
                    (
                      p,
                      commonAttributeWidthPriority,
                      opPriority,
                      posPriority
                    )
                  }
              }
              .sortBy(f => (f._2, f._3, f._4))
              .reverse

            val baseChild = containedAtLeastOneChildren.head._1
            val indexChild = children.diff(Seq(baseChild)).head
            childrenToAdd = baseChild :: indexChild :: Nil

            //DEBUG
//            println(
//              s"containedAtLeastOneChildren:${containedAtLeastOneChildren}"
//            )
//            println(s"baseChild:${baseChild}")
//            println(s"indexChild:${indexChild}")

            preferenceMap(baseChild) = (
              Some(c),
              preferredOrderOfParent.intersect(baseChild.outputOld.toSet),
              parentPreferenceLevel
            )

            preference._3 match {
              case PreferenceLevel.SatisfyAll =>
                preferenceMap(indexChild) = (
                  Some(c),
                  preferredOrderOfParent.intersect(indexChild.outputOld.toSet),
                  PreferenceLevel.SatisfyAll
                )
              case PreferenceLevel.SatisfyOne =>
                preferenceMap(indexChild) = (
                  Some(c),
                  preferredOrderOfParent.intersect(indexChild.outputOld.toSet),
                  PreferenceLevel.MaybeSatisfy
                )
              case PreferenceLevel.MaybeSatisfy =>
                preferenceMap(indexChild) = (
                  Some(c),
                  preferredOrderOfParent.intersect(indexChild.outputOld.toSet),
                  PreferenceLevel.MaybeSatisfy
                )
            }

            //add base and index information to help transform plan
            baseAndIndexInBinaryJoinMap(c) = (baseChild, indexChild)

            //add children to queue
            childrenToAdd
          case _ => Seq()
        }
      }
    }

    (preferenceMap, baseAndIndexInBinaryJoinMap)
  }

  def genAttributeOrder(): mutable.ArrayBuffer[String] = {
    val attributeOrder = mutable.ArrayBuffer[String]()
    val producedAttributes = mutable.ArrayBuffer[String]()
    val planToTraverse = rootPlanWithBaseAndIndexDetermined
    val (preferenceMap, _) = determinePreferenceAndBaseIndexInBinary(
      planToTraverse
    )

    rootPlanWithBaseAndIndexDetermined.foreach { child =>
      val preferenceOption = preferenceMap.get(child)
      preferenceOption match {

        case Some(preference) =>
          val preferredAttributes = preference._2
          val preferenceLevel = preference._3
          child match {
            case a: LocalSemiringAggregateExec =>
              producedAttributes += a.producedAttribute
            case j @ LocalJoinExec(children, JoinType.GHD, _)
                if children.forall(_.isInstanceOf[LocalPlaceHolderExec]) =>
              //compute the optimal attribute order under constraint
              val schemas = j.children.map(_.outputOld)
              val statistics = schemas
                .zip(
                  j.children
                    .map(_.asInstanceOf[LocalPlaceHolderExec])
                    .map(_.pos)
                    .map(placeHolderStatistics)
                )
                .toMap
              val attributeOrderComputer =
                new LeapFrogOrderComputer(schemas, statistics)

              val localAttributeOrder =
                attributeOrderComputer.optimalOrderUnderPreferredAttributes(
                  preferredAttributes,
                  preferenceLevel
                )

//              //DEBUG
//              pprint.pprintln(statistics)
//              pprint.pprintln(attributeOrder)
//              pprint.pprintln(localAttributeOrder)
//              pprint.pprintln(preferredAttributes)
//              pprint.pprintln(preferenceLevel)

              attributeOrder ++= (localAttributeOrder._1.diff(attributeOrder))
            case p: LocalPlaceHolderExec =>
              //compute the optimal attribute order under constraint
              val commonPreferredAttributes =
                preferredAttributes.intersect(p.outputOld.toSet)
              val commonPreferredAttributeSize = commonPreferredAttributes.size

              if (preferenceLevel == PreferenceLevel.SatisfyAll) {

                //DEBUG
                if (commonPreferredAttributes != preferredAttributes) {
                  println(p)
                }

                assert(
                  commonPreferredAttributes == preferredAttributes,
                  s"commonPreferredAttributes:${commonPreferredAttributes} is not equal to preferredAttributes:${preferredAttributes}"
                )
              }

              val localAttributeOrder = p.outputOld.permutations
                .filter { attributeOrder =>
                  attributeOrder
                    .take(commonPreferredAttributeSize)
                    .toSet
                    .subsetOf(commonPreferredAttributes)
                }
                .toSeq
                .head

              attributeOrder ++= (localAttributeOrder.diff(attributeOrder))
            case _ =>
          }

        case None =>
      }

    }

    attributeOrder ++= producedAttributes

    attributeOrder
  }

  def genProcessedLocalPlan(): LocalProcessingExec = {
    val optimizedAttributeOrder = genAttributeOrder()
    sharedAttributeOrder ++= optimizedAttributeOrder

    val postProcessedRootPlan = rootPlanWithBaseAndIndexDetermined transform {
      case p: LocalPlaceHolderExec =>
        p.copy(outputOld = p.localAttributeOrder)
    }

    postProcessedRootPlan.asInstanceOf[LocalProcessingExec]
  }

}
