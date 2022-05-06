package org.apache.spark.secco.parsing

import org.apache.spark.secco.catalog.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.secco.expression.Expression
import org.apache.spark.secco.optimization.LogicalPlan
import org.apache.spark.secco.expression.NamedExpression
import org.apache.spark.secco.analysis.UnresolvedPattern
import org.apache.spark.secco.types.{DataType, StructType}

/** Interface for a parser.
  */
trait ParserInterface {

  /** Parse a string to a [[LogicalPlan]].
    */
  def parsePlan(sqlText: String): LogicalPlan

  /** Parse a string to an [[Expression]].
    */
  def parseExpression(sqlText: String): Expression

  /** Parse a string to an [[NamedExpression]]. */
  def parseNamedExpression(sqlText: String): Expression

  /** Parse a string to a [[UnresolvedPattern]] */
  def parsePatternExpression(sqlText: String): Expression

  /** Parse a string to a [[TableIdentifier]].
    */
  def parseTableIdentifier(sqlText: String): TableIdentifier

  /** Parse a string to a [[FunctionIdentifier]].
    */
  def parseFunctionIdentifier(sqlText: String): FunctionIdentifier

  /** Parse a string to a [[StructType]]. The passed SQL string should be a comma separated list
    * of field definitions which will preserve the correct Hive metadata.
    */
  def parseTableSchema(sqlText: String): StructType

  /** Parse a string to a [[DataType]].
    */
  def parseDataType(sqlText: String): DataType

}
