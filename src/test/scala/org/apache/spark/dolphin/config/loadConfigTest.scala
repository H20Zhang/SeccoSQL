//package org.apache.spark.dolphin.config
//
//import org.scalatest._
//
//class loadConfigTest extends FunSuite with BeforeAndAfter {
//  val testDataValue: Long = (1024 * 1024).toLong // used as the value to test KB\MB\GB set and get methods
//  val testTime: Long = (30).toLong // used as the value to test Seconds\Minutes\Hours set and get methods
//  val durationUnit: Double = 1e3 // ms
//  val dataUnit: Double = 1e3 // Byte
//  var config: Config = Config.loadConfig("referencefortest.conf")
//  before {
//    config = Config.loadConfig("referencefortest.conf")
//  }
//
//  test("set and get if any type is given explicitly") {
//    // When the key-value pair is set manually,the manually set value will be got
//    config.set[Int]("dolphin.num_partition", 1)
//    assert(config.get[Int]("dolphin.num_partition") == 1)
//
//    // And get default value if the key is not set"
//    assert(config.get[Int]("dolphin.num_core") == 196)
//  }
//
//  test("get and set Int") {
//    // When the key-value pair is set manually,the manually set value will be got
//    config.setInt("dolphin.num_partition", 1)
//    assert(config.getInt("dolphin.num_partition") == 1)
//
//    // And get default value if the key is not set
//    assert(config.getInt("dolphin.num_core") == 196)
//  }
//
//  test("get and set Long") {
//    // When the key-value pair is set manually,the manually set value will be got
//    config.setLong("dolphin.test", 1)
//    val res0: Long = (1).toLong
//    assert(config.getLong("dolphin.test") == res0)
//
//    // And get default value if the key is not set
//    val res1: Long = (196).toLong
//    assert(config.getLong("dolphin.num_core") == res1)
//  }
//  test("get and set Float") {
//    // When the key-value pair is set manually,the manually set value will be got
//    val res0: Float = (1.0).toFloat
//    config.setFloat("dolphin.test", res0)
//    assert(config.getFloat("dolphin.test") == res0)
//
//    // And get default value if the key is not set
//    val res1: Float = (6.66).toFloat
//    assert(config.getFloat("dolphin.optimizer.discount") == res1)
//  }
//
//  test("get and set Double") {
//    // When the key-value pair is set manually,the manually set value will be got
//    config.setDouble("dolphin.test", 11.0)
//    assert(config.getDouble("dolphin.test") == 11.0)
//
//    // And get default value if the key is not set
//    assert(config.getDouble("dolphin.optimizer.discount") == 6.66)
//  }
//
//  test("get and set String") {
//    // When the key-value pair is set manually,the manually set value will be got
//    val res0: String = "the is a test value"
//    config.setString("dolphin.test", res0)
//    assert(config.getString("dolphin.test") == res0)
//
//    // And get default value if the key is not set")
//    assert(config.getString("dolphin.optimizer.delay_strategy") == "Heuristic")
//  }
//
//  test("get and set Data in KB/MB/GB unit") {
//    // When the key-value pair is set manually,the manually set value will be got
//    val res0: Long = (1024).toLong
//    config.setSizeAsKB("data.forSet", res0) // lgh: data.forSet == 1024
//    config.setSizeAsMB("data.forSet2", res0) // lgh: data.forSet2 == 1024 * 1024
//    config.setSizeAsGB("data.forSet3", res0) // lgh: data.forSet3 == 1024 * 1024 * 1024
//
//    assert(config.getSizeAsKB("data.forSet") == res0)
//    assert(config.getSizeAsMB("data.forSet2") == res0)
//    assert(config.getSizeAsGB("data.forSet3") == res0)
//
//    // And get default value if the key is not set
//    val res1: Long = (1048576).toLong
//    assert(config.getSizeAsKB("data.value") == (res1 / dataUnit).round)
//    assert(config.getSizeAsMB("data.value") == (res1 / (dataUnit * dataUnit)).round)
//    assert(config.getSizeAsGB("data.value") == (res1 / (dataUnit * dataUnit * dataUnit)).round)
//  }
//
//  test("get and set duration in Seconds/Minutes/Hours unit") {
//    // When the key-value pair is set manually,the manually set value will be got
//    val res0: Long = (1024).toLong
//    config.setDurationAsSeconds("time.forSet", res0)
//    config.setDurationAsMinutes("time.forSet2", res0)
//    config.setDurationAsHours("time.forSet3", res0)
//
//    assert(config.getDurationAsSeconds("time.forSet") == res0)
//    assert(config.getDurationAsMinutes("time.forSet2") == res0)
//    assert(config.getDurationAsHours("time.forSet3") == res0)
//
//    // And get default value if the key is not set
//    val res1: Long = (3600).toLong
//    assert(config.getDurationAsSeconds("time.value") == (res1 / durationUnit).round)
//    assert(config.getDurationAsMinutes("time.value") == (res1 / (60 * durationUnit) ).round)
//    assert(config.getDurationAsHours("time.value") == (res1 / (3600 * durationUnit)).round)
//  }
//
//  test("tell you if it contains specified key-value pair") {
//    // When the key-value pair is set manually,the manually set value will be got
//    config.set[Int]("test", 1)
//    assert(config.Contains("test"))
//
//    // And return true if the key-pair is in default settings
//    assert(config.Contains("dolphin.optimizer.max_iteration"))
//  }
//}
