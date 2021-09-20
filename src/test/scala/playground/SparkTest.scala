//package playground
//
//import org.apache.spark.sql.SparkSession
//
//object SparkTest extends App {
//  val builder = SparkSession.builder
//  val spark = builder
//    .config("spark.sql.codegen.wholeStage", false)
//    .master("local[1]")
//    .appName("sparkTest")
//    .getOrCreate()
//  import spark.sqlContext.implicits._
//  case class j(i: Int, k: Int)
//  val sc = spark.sparkContext
//  sc.parallelize(1 to 9).map(x => j(x, x)).toDF.createOrReplaceTempView("j")
//  spark.sql("select * from j").explain()
//  println(spark.sql("select * from j").collect.toSeq)
//
//}
