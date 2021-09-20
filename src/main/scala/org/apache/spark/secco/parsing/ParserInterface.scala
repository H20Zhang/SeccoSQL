package org.apache.spark.secco.parsing

import org.apache.spark.secco.catalog.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.optimization.LogicalPlan

/**
  * Interface for a parser.
  */
trait ParserInterface {

  /**
    * Parse a string to a [[LogicalPlan]].
    */
  def parsePlan(sqlText: String): LogicalPlan

  /**
    * Parse a string to an [[Expression]].
    */
  def parseExpression(sqlText: String): Expression

  /**
    * Parse a string to a [[TableIdentifier]].
    */
  def parseTableIdentifier(sqlText: String): TableIdentifier

  /**
    * Parse a string to a [[FunctionIdentifier]].
    */
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier

}
