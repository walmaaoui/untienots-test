name := "untie-nots-test"

version := "0.1"

scalaVersion := "2.12.8"

lazy val `file-to-kafka` = project.settings(
  libraryDependencies ++= Seq(
    Dependencies.sparkMLLib,
    Dependencies.sparkSQLKafka,
    Dependencies.scopt,
    Dependencies.typeSafeConfig,
    Dependencies.scalaLogging,
    Dependencies.scalaTest,
    Dependencies.sparkTestingBase
  ))

lazy val `words-stream-processing` = project.settings(
    libraryDependencies ++= Seq(
      Dependencies.sparkSQL,
      Dependencies.sparkSQLKafka,
      Dependencies.scopt,
      Dependencies.typeSafeConfig,
      Dependencies.scalaLogging,
      Dependencies.scalaTest
    ))

lazy val `kafka-to-parquet` =
  project.settings(
    libraryDependencies ++= Seq(
      Dependencies.sparkSQL,
      Dependencies.sparkSQLKafka,
      Dependencies.scopt,
      Dependencies.typeSafeConfig,
      Dependencies.scalaLogging,
      Dependencies.scalaTest
    ))

lazy val `analytics` =
  project.settings(
    libraryDependencies ++= Seq(
      Dependencies.sparkSQL,
      Dependencies.scopt,
      Dependencies.typeSafeConfig,
      Dependencies.scalaLogging,
      Dependencies.scalaTest,
      Dependencies.sparkTestingBase
    ))

lazy val root = (project in file("."))
  .aggregate(`file-to-kafka`, `words-stream-processing`, `kafka-to-parquet`, `analytics`)
