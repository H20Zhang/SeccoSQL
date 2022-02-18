package util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.secco.SeccoSession
import org.apache.spark.secco.catalog.Catalog
import org.apache.spark.secco.util.misc.SparkSingle
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

class SeccoFunSuite
    extends FunSuite
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

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
