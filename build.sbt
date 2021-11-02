import scala.sys.process._

name := "SeccoSQL"
version := "0.1"
scalaVersion := "2.11.12"

/** Dependency */

//Resolver
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
// Spark
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided"
//libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
// Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
// Util
libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.3"
libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.1.0"
// Math Optimizer
libraryDependencies += "com.joptimizer" % "joptimizer" % "5.0.0"
// Graph Processing
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.3.0"
// Args Parsing
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
// Configuration
libraryDependencies += "com.typesafe" % "config" % "1.4.0"
// Better Printing
libraryDependencies += "com.lihaoyi" %% "pprint" % "0.5.4"
// Metering
libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.7"

watchSources += baseDirectory.value / "script/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

test in assembly := {}
//testOptions in Test += Tests.Cleanup(() => SparkSingle)
parallelExecution in Test := false

/*Custom tasks*/
lazy val upload = taskKey[Unit]("Upload the files")
upload := {
  "./script/upload.sh" !
}

lazy val assembleThenUpload = taskKey[Unit]("Upload the jar after assembly")
assembleThenUpload := {
  assembly.value
  "./script/upload.sh" !
}

//testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework")

//fork := true
//outputStrategy := Some(StdoutOutput)
//connectInput := true
//logBuffered := false
//assemblyExcludedJars in assembly := {
//  val cp = (fullClasspath in assembly).value
//  cp.filter { f =>
//    f.data.getName == "systemml-1.1.0.jar" ||
//      f.data.getName == "SCPSolver.jar" ||
//      f.data.getName == "LPSOLVESolverPack.jar" ||
//      f.data.getName == "GLPKSolverPack.jar"
//  }
//}
