import sbt._

object Dependencies {
  val sparkVersion = "2.4.5"
  val sparkMLLib = "org.apache.spark" %% "spark-mllib" % sparkVersion
  val sparkSQLKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
  val sparkSQL = "org.apache.spark" %% "spark-sql" % "2.4.5"
  val typeSafeConfig = "com.typesafe" % "config" % "1.4.0"
  val scopt = "com.github.scopt" %% "scopt" % "4.0.0-RC2"
  val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8" % "test"
  val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % "test" //2.4.5 is not available yet
}
