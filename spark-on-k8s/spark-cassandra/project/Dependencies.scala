object Dependencies {

  import sbt._

  private val SparkGroupId: String = "org.apache.spark"
  private val SparkVersion: String = "2.4.0"

  val SparkCore: ModuleID = SparkGroupId %% "spark-core" % SparkVersion
  val SparkSql: ModuleID = SparkGroupId %% "spark-sql" % SparkVersion
  val CassandraConnector: ModuleID = "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.1"
  val Scalatest: ModuleID = "org.scalatest" %% "scalatest" % "3.0.7"
  val JUnit: ModuleID = "junit" % "junit" % "4.12"
}
