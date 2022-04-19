package util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.secco.analysis.UnresolvedAttribute
import org.apache.spark.secco.{SeccoDataFrame, SeccoSession}
import org.apache.spark.secco.catalog.Catalog
import org.apache.spark.secco.execution.plan.communication.ShareValues
import org.apache.spark.secco.execution.storage.row.{
  InternalRow,
  UnsafeInternalRow
}
import org.apache.spark.secco.expression.utils.AttributeMap
import org.apache.spark.secco.expression.{Attribute, AttributeReference}
import org.apache.spark.secco.optimization.util.EquiAttributes
import org.apache.spark.secco.types.{
  DataTypes,
  StructField,
  StructType,
  TypeInfer
}
import org.apache.spark.secco.util.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class SeccoFunSuite
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  /** Create dummy attribute for testing. */
  def createTestAttribute(name: String): Attribute = {
    AttributeReference(name, DataTypes.IntegerType)()
  }

  /** Create an empty relation for testing.
    * @param name name of the relation.
    * @param schema schema of the relation in strings. Note: the default Datatype is IntegerType.
    * @param primaryKeyNames the name of primary keys in schema.
    */
  def createTestEmptyRelation(name: String, schema: String*)(
      primaryKeyNames: String*
  ): Unit = {

    // Sanity check.
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

  /** Create test relation with local rows */
  def createTestRelation(rows: Seq[Seq[Any]], name: String, schema: String*)(
      primaryKeyNames: String*
  ): Unit = {

    // Sanity check.
    assert(
      schema.nonEmpty,
      "Numbers of attribute used to create dummy relation must >= 1."
    )

    assert(
      rows.nonEmpty,
      "Numbers of rows used to create test relation must >= 1."
    )

    val headRow = rows.head
    assert(
      headRow.size == schema.size,
      s"Width of row:${headRow.size} and schema:${schema.size} cannot match."
    )

    assert(
      rows.forall(row => row.size == headRow.size),
      "Inconsistency in schema of the first row and the rest of rows."
    )

    // Try to infer the schema from the first row.
    val dataTypes = headRow.map(x => TypeInfer.inferType(x))

    val attributesMap = schema
      .zip(dataTypes)
      .map { case (attrName, datatype) =>
        (attrName, StructField(attrName, datatype))
      }
      .toMap

    // Build InternalRows
    val internalRows = rows.map { row => InternalRow(row: _*) }

    SeccoDataFrame
      .fromSeq(
        internalRows,
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
