import Dependencies._

name := "spark-cassandra"

version := "0.0.1"

scalaVersion := "2.11.12"

parallelExecution in Test := false

libraryDependencies ++= Seq(
  SparkCore,
  SparkSql,
  CassandraConnector,
  Scalatest % Test,
  JUnit % Test)

val meta = "META.INF(.)*".r
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case _ => MergeStrategy.first
}