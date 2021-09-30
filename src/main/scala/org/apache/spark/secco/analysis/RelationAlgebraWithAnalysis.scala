package org.apache.spark.secco.analysis

import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.CatalogTable
import org.apache.spark.secco.optimization.plan._
import org.apache.spark.secco.optimization.{ExecMode, LogicalPlan}

/** An set of RA primitives for constructing RA expression. */
object RelationAlgebraWithAnalysis {

  def dlSession = SeccoSession.currentSession

  implicit def R(option: String): LogicalPlan = {

    val extractRegex = "(\\w*)(\\(.*\\))".r

    val groups = extractRegex.findFirstMatchIn(option).get
    val catalog = dlSession.sessionState.catalog

    if (groups.groupCount < 2) {
      val tableName = groups.group(1)

      if (
        catalog
          .getTable(tableName)
          .nonEmpty
      ) {
        Relation(tableName)
      } else {
        throw new Exception(s"no such table or view $tableName")
      }
    } else {
      val tableName = groups.group(1)

      val attrExtractRegex = "[\\w|\\[|\\]]+".r
      val attrs = attrExtractRegex.findAllIn(groups.group(2)).toSeq
      val tableNamePlusAttrs = tableName +: attrs

      //create new table
      catalog.createTable(CatalogTable(tableNamePlusAttrs: _*))
      Relation(tableName)
    }

  }

  def select(option: String, input: LogicalPlan): LogicalPlan = {

    val selectionExprString = option.split("and")
    val regex = "(\\w+)\\s*([<|>|=]?)(!=)?\\s*(\\w+)".r
    assert(
      selectionExprString.forall(str => regex.findFirstMatchIn(str).nonEmpty),
      s"selectionExpr:$option is invalid, each selectionExpr should be of form A < B"
    )

    val selectionExprs = selectionExprString.map { str =>
      val groups = regex.findFirstMatchIn(str).get
      val lhs = groups.group(1)
      val rhs = groups.group(4)
      val op = {
        if (groups.group(2) == null) groups.group(3)
        else groups.group(2)
      }
      (lhs, op, rhs)
    }

    Filter(input, selectionExprs, ExecMode.Coupled)
  }

  def project(option: String, input: LogicalPlan): LogicalPlan = {
    val validRegex = "(\\w+,)*(\\w+)?".r
    assert(validRegex.findFirstMatchIn(option).nonEmpty)

    Project(
      input,
      "\\w+".r.findAllIn(option).toSeq,
      ExecMode.Coupled
    )
  }

//  def distinct(option: String, input: LogicalPlan): LogicalPlan = {
//    val validRegex = "(\\w,)*(\\w)?".r
//    assert(validRegex.findFirstMatchIn(option).nonEmpty)
//    Project(input, option.split(","), true, ExecMode.Mixed)
//  }

  def transform(option: String, input: LogicalPlan): LogicalPlan = {
    val funcs = option.split(",").map(_.trim)
    val validRegex =
      "(((\\d+\\.\\d+)|(\\w+))(\\*|\\+))*((\\d+\\.\\d+)|(\\w+))".r
    funcs.foreach(clause =>
      assert(validRegex.findFirstMatchIn(clause).nonEmpty)
    )
    Transform(input, funcs, funcs)
  }

  def aggregate(option: String, input: LogicalPlan): LogicalPlan = {
    val aggWithGroupRegex =
      "((count)|(sum)|(min)|(max))\\(([\\w|\\*]+)\\)\\sby\\s((\\w+,\\s*)*\\w+)".r
    val aggWithoutGroupRegex = "((count)|(sum)|(min)|(max))\\(([\\w|\\*]+)\\)".r

    aggWithGroupRegex.findFirstMatchIn(option) match {
      case Some(result) =>
        val semiring = result.group(1)
        val aggAttribute = result.group(6)
        val groupingList = result.group(7)

        Aggregate(
          input,
          groupingList.split(",").map(_.trim),
          (semiring, aggAttribute),
          Seq(),
          ExecMode.Coupled
        )
      case None =>
        aggWithoutGroupRegex.findFirstMatchIn(option) match {
          case Some(result) =>
            val semiring = result.group(1)
            val aggAttribute = result.group(6)

            Aggregate(
              input,
              Seq(),
              (semiring, aggAttribute),
              Seq(),
              ExecMode.Coupled
            )
          case None =>
            throw new Exception(
              s"${this.getClass}.aggregate(option:${option}, _) | option is not valid"
            )
        }
    }

  }

  def union(children: LogicalPlan*) = {
    Union(children, ExecMode.Coupled)
  }

  def diff(left: LogicalPlan, right: LogicalPlan) = {
    Diff(left, right, ExecMode.Coupled)
  }

  def iterative(
      left: LogicalPlan,
      returnTableIdentifier: String,
      numRun: Int = dlSession.sessionState.conf.recursionNumRun
  ) = {
    Iterative(left, returnTableIdentifier, numRun)
  }

  def assign(tableName: String, plan: LogicalPlan) = {
    Assign(plan, tableName)
  }

  def update(
      tableName: String,
      deltaTableName: String,
      key: Seq[String],
      plan: LogicalPlan
  ) = {
    Update(plan, tableName, deltaTableName, key)
  }

  def rename(option: String, plan: LogicalPlan) = {
    val attrRenameExtractRegex = "\\w+/\\w+".r
    val outputExtractRegex = "\\w+".r

    val attrRenameMap = attrRenameExtractRegex.findFirstMatchIn(option) match {
      case Some(_) =>
        attrRenameExtractRegex
          .findAllIn(option)
          .toList
          .map(_.split("/"))
          .map(f => f(1) -> f(0))
          .toMap
      case None =>
        val newOutput = outputExtractRegex.findAllIn(option).toList
        val oldOutput = plan.outputOld
        assert(
          newOutput.size == oldOutput.size,
          s"newOutput.size:${newOutput.size} is not equal to oldOutput.size:${oldOutput.size}"
        )
        oldOutput.zip(newOutput).filter(f => f._1 != f._2).toMap
    }

    Rename(plan, attrRenameMap)

  }

  def join(children: LogicalPlan*) = {
    MultiwayNaturalJoin(children, JoinType.Natural, ExecMode.Coupled)
  }

  def cartesianProduct(children: LogicalPlan*) = {
    CartesianProduct(children, ExecMode.Coupled)
  }

  def thetaJoin(option: String, children: LogicalPlan*) = {
    select(option, CartesianProduct(children, ExecMode.Coupled))
  }

}
