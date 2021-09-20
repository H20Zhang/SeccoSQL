package unit.config

import org.apache.spark.secco.config.SeccoConfiguration
import util.SeccoFunSuite

class SeccoConfigurationSuite extends SeccoFunSuite {

  val config = SeccoConfiguration.newDefaultConf()
  val testDataValue: Long =
    (1024 * 1024).toLong // used as the value to test KB\MB\GB set and get methods
  val testTime: Long =
    (30).toLong // used as the value to test Seconds\Minutes\Hours set and get methods
  val durationUnit: Double = 1e3 // ms
  val dataUnit: Double = 1e3 // Byte

  test("check set") {
    config.set("MyTest", "1")
    assert(config.get("MyTest") == "1")
  }

  test("check setInt and getInt") {
    config.setInt("Int", 1)
    val a: Int = 1
    assert(config.getInt("Int") == a)
  }

  test("check setLong and getLong") {
    config.setLong("Long", 1)
    val a: Long = 1
    assert(config.getLong("Long") == a)
  }

  test("check setFloat and getFloat") {
    config.setFloat("Float", 1)
    val a: Float = (1.0).toFloat
    assert(config.getFloat("Float") == a)
  }

  test("check setDouble and getDouble") {
    config.setDouble("Double", 1)
    val a: Double = (1.0).toDouble
    assert(config.getDouble("Double") == a)
  }

  test("check setString and getString") {
    config.setString("String", "String")
    assert(config.getString("String") == "String")
  }

  test("check setSizeAsKB and getSizeAsKB") {
    config.setSizeAsKB("KB", testDataValue)
    assert(config.getSizeAsKB("KB") == testDataValue)
  }

  test("check setSizeAsMB and getSizeAsMB") {
    config.setSizeAsMB("MB", testDataValue)
    assert(config.getSizeAsMB("MB") == testDataValue)
  }

  test("check setSizeAsGB and getSizeAsGB") {
    config.setSizeAsGB("GB", testDataValue)
    assert(config.getSizeAsGB("GB") == testDataValue)
  }

  test("check setDurationAsSeconds and getDurationAsSeconds") {
    config.setDurationAsSeconds("Seconds", testTime)
    assert(config.getDurationAsSeconds("Seconds") == testTime)
  }

  test("check setDurationAsMinutes and getDurationAsMinutes") {
    config.setDurationAsMinutes("Minutes", testTime)
    assert(config.getDurationAsMinutes("Minutes") == testTime)
  }

  test("check setDurationAsHours and getDurationAsHours") {
    config.setDurationAsHours("Hours", testTime)
    assert(config.getDurationAsHours("Hours") == testTime)
  }

  test("check set and get separately") {
    config.set("val1", "true")
    assert(config.getBoolean("val1"))

    config.set("val2", "20")
    assert(config.getInt("val2") == 20)

    config.set("val3", "30")
    assert(config.getLong("val3") == 30L)

    config.set("val4", "40")
    assert(config.getFloat("val4") == 40f)

    config.set("val5", "50")
    assert(config.getDouble("val5") == 50d)
  }
}
