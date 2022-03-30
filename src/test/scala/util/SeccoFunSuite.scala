package util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.secco.{SeccoDataFrame, SeccoSession}
import org.apache.spark.secco.catalog.Catalog
import org.apache.spark.secco.expression.Attribute
import org.apache.spark.secco.types.{DataTypes, StructField, StructType}
import org.apache.spark.secco.util.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class SeccoFunSuite
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  /** Create an empty relation with `name` and `schema` for testing. */
  def createDummyRelation(name: String, schema: String*)(
      primaryKeyNames: String*
  ): Unit = {

    assert(
      schema.nonEmpty,
      "Numbers of attribute used to create dummy relation must >= 1."
    )

    val attributesMap = schema.map { attrName =>
      (attrName, StructField(attrName, DataTypes.IntegerType))
    }.toMap

    SeccoDataFrame
      .empty(
        StructType(
          schema.map(attrName => attributesMap(attrName))
        ),
        primaryKeyNames = primaryKeyNames
      )
      .createOrReplaceTable(name)
  }

  def setupDB() = {}

  def seccoSession = SeccoSession.currentSession

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SeccoSession.setCurrentSession(SeccoSession.newDefaultSession)
    setupDB()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }

  override protected def afterAll(): Unit = {
    SparkSingle.close()
  }

  def clearSession(): Unit = {
    super.beforeAll()
    SeccoSession.setCurrentSession(SeccoSession.newDefaultSession)
    setupDB()
  }

}
