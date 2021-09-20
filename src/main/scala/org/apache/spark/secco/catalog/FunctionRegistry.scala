package org.apache.spark.secco.catalog

import org.apache.spark.secco.analysis.NoSuchFunctionException
import org.apache.spark.secco.catalog.FunctionRegistry.FunctionBuilder
import org.apache.spark.secco.expression._
import org.apache.spark.secco.expression.aggregate._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** A catalog for looking up user defined functions, used by an [[Analyzer]].
  *
  * Note: The implementation should be thread-safe to allow concurrent access.
  */
trait FunctionRegistry {

  /** register the function in catalog */
  def registerFunction(name: String, builder: FunctionBuilder)

  /** loop up and create the expression based on name and children */
  def lookUpFunction(name: String, children: Seq[Expression]): Expression

  /** list existing function in catalog */
  def listFunction(): Seq[String]

  /** look up the builder for building the expression */
  def lookUpFunctionBuilder(name: String): Option[FunctionBuilder]

  /** check if the function with a given name exists */
  def functionExists(name: String): Boolean =
    lookUpFunctionBuilder(name).isDefined

  /** drop a function and return whether the function existed */
  def dropFunction(name: String): Boolean

  /** clear all registered functions */
  def clear(): Unit
}

class SimpleFunctionRegistry extends FunctionRegistry {

  protected val functionBuilderMap = mutable.HashMap[String, FunctionBuilder]()

  /** register the function in catalog */
  override def registerFunction(name: String, builder: FunctionBuilder): Unit =
    functionBuilderMap.put(name, builder)

  /** loop up and create the expression based on name and children */
  override def lookUpFunction(
      name: String,
      children: Seq[Expression]
  ): Expression = {
    val funcBuilder = functionBuilderMap.get(name).getOrElse {
      throw new NoSuchFunctionException("functionRegistry", name)
    }
    funcBuilder(children)
  }

  /** list existing function in catalog */
  override def listFunction(): Seq[String] = {
    functionBuilderMap.keysIterator.toList.sorted
  }

  /** look up the builder for building the expression */
  override def lookUpFunctionBuilder(name: String): Option[FunctionBuilder] =
    functionBuilderMap.get(name)

  /** drop a function and return whether the function existed */
  override def dropFunction(name: String): Boolean =
    functionBuilderMap.remove(name).isDefined

  /** clear all registered functions */
  override def clear(): Unit = functionBuilderMap.clear()
}

object FunctionRegistry {
  type FunctionBuilder = Seq[Expression] => Expression

  val expressions: Map[String, FunctionBuilder] = Map(
    //arithmetic operators
    expression[Add]("+"),
    expression[Subtract]("-"),
    expression[Multiply]("*"),
    expression[Divide]("/"),
    expression[Remainder]("%"),
    expression[UnaryMinus]("negative"),
    expression[UnaryPositive]("positive"),
    //predicates
    expression[And]("and"),
//    expression[In]("in"),
    expression[Not]("not"),
    expression[Or]("or"),
    //comparison operators
    expression[EqualTo]("="),
    expression[EqualTo]("=="),
    expression[GreaterThan](">"),
    expression[GreaterThanOrEqual](">="),
    expression[LessThan]("<"),
    expression[LessThanOrEqual]("<="),
    expression[Not]("!"),
    //aggregate functions
    expression[Min]("min"),
    expression[Max]("max"),
    expression[Sum]("sum"),
    expression[Average]("average"),
    expression[Count]("count")
  )

  def newBuiltin: SimpleFunctionRegistry = {
    val fr = new SimpleFunctionRegistry
    expressions.foreach { case (name, builder) =>
      fr.registerFunction(name, builder)
    }
    fr
  }

  val functionSet: Set[String] = newBuiltin.listFunction().toSet

  private def expression[T <: Expression](
      name: String
  )(implicit tag: ClassTag[T]): (String, FunctionBuilder) = {

    val constructors = tag.runtimeClass.getConstructors

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor =
      constructors.find(_.getParameterTypes.toSeq == Seq(classOf[Seq[_]]))
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(
          varargCtor.get.newInstance(expressions).asInstanceOf[Expression]
        ) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new Exception(e.getCause.getMessage)
        }
      } else {
        // Otherwise, find a constructor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f =
          constructors.find(_.getParameterTypes.toSeq == params).getOrElse {
            throw new Exception(
              s"Invalid number of arguments for function $name"
            )
          }
        Try(f.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) =>
            // the exception is an invocation exception. To get a meaningful message, we need the
            // cause.
            throw new NoSuchFunctionException("", name)
        }
      }
    }

    (name, builder)
  }
}
