//package org.apache.spark.dolphin.expression.aggregate
//
//import org.apache.spark.dolphin.execution.storage.row.InternalRow
//import org.apache.spark.dolphin.expression.Expression
//import org.apache.spark.dolphin.types.DataType
//
///**
//  * Return the minimum value of `expr`
//  * @param child the child `expr`
//  */
//case class Min(child: Expression) extends AggregateFunction {
//  override def nullable: Boolean = true
//
//  override def eval(input: InternalRow): Any = ???
//
//  override def dataType: DataType = child.dataType
//
//  override def children: Seq[Expression] = child :: Nil
//}
//
///**
//  * Return the max value of `expr`
//  * @param child the child `expr`
//  */
//case class Max(child: Expression) extends AggregateFunction {
//  override def nullable: Boolean = true
//
//  override def eval(input: InternalRow): Any = ???
//
//  override def dataType: DataType = child.dataType
//
//  override def children: Seq[Expression] = child :: Nil
//}
//
///**
//  * Return the numbers of value of `expr`
//  * @param child the child `expr`
//  */
//case class Count(child: Expression) extends AggregateFunction {
//  override def nullable: Boolean = true
//
//  override def eval(input: InternalRow): Any = ???
//
//  override def dataType: DataType = child.dataType
//
//  override def children: Seq[Expression] = child :: Nil
//}
//
///**
//  * Return the average value of `expr`
//  * @param child the child `expr`
//  */
//case class Average(child: Expression) extends AggregateFunction {
//  override def nullable: Boolean = true
//
//  override def eval(input: InternalRow): Any = ???
//
//  override def dataType: DataType = child.dataType
//
//  override def children: Seq[Expression] = child :: Nil
//}
//
///**
//  * Return the sum value of `expr`
//  * @param child the child `expr`
//  */
//case class Sum(child: Expression) extends AggregateFunction {
//  override def nullable: Boolean = true
//
//  override def eval(input: InternalRow): Any = ???
//
//  override def dataType: DataType = child.dataType
//
//  override def children: Seq[Expression] = child :: Nil
//}
