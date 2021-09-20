//package org.apache.spark.dolphin.config
//
//import org.scalatest.{FunSuite,BeforeAndAfter}
//
//class defaultConfigTest extends FunSuite with BeforeAndAfter{
//  val testDataValue: Long = (1024*1024).toLong // used as the value to test KB\MB\GB set and get methods
//  val testTime:Long = (30).toLong // used as the value to test Seconds\Minutes\Hours set and get methods
//  val durationUnit: Double = 1e3 // ms
//  val dataUnit: Double = 1e3 // Byte
//  var config:Config=Config.defaultConfig()
//  before{
//    config=Config.defaultConfig()
//  }
//
//  test("set and get if any type is given explicitly"){
//    config.set[Int]("MyTest",1)
//    assert(config.get[Int]("MyTest")==1)
//  }
//
//  test("setInt and getInt"){
//    config.setInt("Int",1)
//    val a:Int = 1
//    assert(config.getInt("Int")==a)
//  }
//
//  test("setLong and getLong"){
//    config.setLong("Long",1)
//    val a:Long = 1
//    assert(config.getLong("Long")==a)
//  }
//
//  test("setFloat and getFloat"){
//    config.setFloat("Float",1)
//    val a:Float = (1.0).toFloat
//    assert(config.getFloat("Float")==a)
//  }
//
//  test("setDouble and getDouble"){
//    config.setDouble("Double",1)
//    val a:Double = (1.0).toDouble
//    assert(config.getDouble("Double")==a)
//  }
//
//  test("setString and getString"){
//    config.setString("String","String")
//    assert(config.getString("String")=="String")
//  }
//
//  test("setSizeAsKB and getSizeAsKB"){
//    config.setSizeAsKB("KB",testDataValue)
//    assert(config.getSizeAsKB("KB")==testDataValue)
//  }
//
//  test("setSizeAsMB and getSizeAsMB"){
//    config.setSizeAsMB("MB",testDataValue)
//    assert(config.getSizeAsMB("MB")==testDataValue)
//  }
//
//  test("setSizeAsGB and getSizeAsGB"){
//    config.setSizeAsGB("GB",testDataValue)
//    assert(config.getSizeAsGB("GB")==testDataValue)
//  }
//
//  test("setDurationAsSeconds and getDurationAsSeconds"){
//    config.setDurationAsSeconds("Seconds",testTime)
//    assert(config.getDurationAsSeconds("Seconds")==testTime)
//  }
//
//  test( "setDurationAsMinutes and getDurationAsMinutes"){
//    config.setDurationAsMinutes("Minutes",testTime)
//    assert(config.getDurationAsMinutes("Minutes")==testTime)
//  }
//
//  test("setDurationAsHours and getDurationAsHours"){
//    config.setDurationAsHours("Hours",testTime)
//    assert(config.getDurationAsHours("Hours")==testTime)
//  }
//
//  test("test bundle"){
//    // Int
//    config.setInt("dolphin.num_core",2)
//    assert(config.asInstanceOf[MyConfig].NUM_CORE == 2)
//
//    // String
//    config.setString("dolphin.optimizer.delay_strategy","force")
//    assert(config.asInstanceOf[MyConfig].DELAY_STRATEGY == "force")
//
//    // Boolean
//    assert(config.asInstanceOf[MyConfig].IS_YARN)
//    config.set[Boolean]("dolphin.is_yarn",false)
//    assert(!config.get[Boolean]("dolphin.is_yarn"))
//
//    // set Duration using string representation
//    config.setString("dolphin.timeout","1h")
//    assert(config.getDurationAsHours("dolphin.timeout")==1)
//
//    // specified size keys
//    assert(config.asInstanceOf[MyConfig].BUDGET==350*dataUnit*dataUnit)
//    config.setSizeAsMB("dolphin.hcube.budget",200)
//    assert(config.asInstanceOf[MyConfig].BUDGET==200*dataUnit*dataUnit)
//    assert(config.getSizeAsMB("dolphin.hcube.budget")==200)
//  }
//
//  test( "tell you if it contains specified key-value pair"){
//    config.set[Int]("containTest",1)
//    assert(config.Contains("containTest"))
//  }
//}
//
