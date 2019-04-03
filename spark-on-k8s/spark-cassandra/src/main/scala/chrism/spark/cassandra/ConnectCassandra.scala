package chrism.spark.cassandra

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ThreadLocalRandom
import java.{sql => js}

import org.apache.spark.sql.SparkSession

import scala.collection.immutable.ListMap

object ConnectCassandra {

  import com.datastax.spark.connector.cql.{CassandraConnectorConf => ccc}

  private val DefaultMaster: String = "local"
  private val DefaultPort: Int = ccc.ConnectionPortParam.default
  private val DefaultNumRows: Int = 10

  private val Operations: Set[Operation] = Set(Read, Write)
  private val HelpShortOpt: String = "-h"
  private val HelpLongOpt: String = "--help"
  private val SparkMasterLongOpt: String = "--spark-master"
  private val CassandraHostLongOpt: String = "--cassandra-host"
  private val CassandraPortLongOpt: String = "--cassandra-port"
  private val TableLongOpt: String = "--table"
  private val KeyspaceLongOpt: String = "--keyspace"
  private val OperationLongOpt: String = "--operation"
  private val NumRowsLongOpt: String = "--num-rows"
  // Using ListMap to keep the options in the order in which they are added.
  private val OptDescriptions: ListMap[String, String] =
    ListMap(
      s"$HelpShortOpt|$HelpLongOpt" -> "Prints this help.",
      SparkMasterLongOpt -> s"Specifies the Spark master (default: $DefaultMaster)",
      CassandraHostLongOpt -> "Specifies the host for the Cassandra cluster.",
      CassandraPortLongOpt -> s"Specifies th port number of the Cassandra cluster (default: $DefaultPort).",
      TableLongOpt -> "Specifies a Cassandra table.",
      KeyspaceLongOpt -> "Specifies the keyspace for the Cassandra table.",
      OperationLongOpt -> s"Specifies one of the operations: ${Operations.mkString(", ")}",
      NumRowsLongOpt -> s"Specifies the number of rows for the operation (default: $DefaultNumRows)."
    )

  private lazy val Separator: String = System.lineSeparator()

  // TODO: add logging

  def main(args: Array[String]): Unit = {
    if (args.contains(HelpShortOpt) || args.contains(HelpLongOpt)) {
      printUsage()
    } else {
      // Naive argument parsing
      if (args.length % 2 != 0) {
        throw new IllegalArgumentException("The number of arguments must be an even number!")
      }

      var master: String = DefaultMaster
      var host: String = null
      var port: Int = DefaultPort
      var keyspace: String = null
      var table: String = null
      var opName: String = null
      var numRows: Int = 10
      args
        .grouped(2)
        .foreach({
          case Array(SparkMasterLongOpt, m)   => master = m
          case Array(CassandraHostLongOpt, h) => host = h
          case Array(CassandraPortLongOpt, p) => port = p.toInt
          case Array(KeyspaceLongOpt, ks)     => keyspace = ks
          case Array(TableLongOpt, t)         => table = t
          case Array(OperationLongOpt, op)    => opName = op
          case Array(NumRowsLongOpt, n)       => numRows = n.toInt
        })

      require(host != null, "Cassandra host name is required!")
      require(keyspace != null, "Cassandra keyspace name is required!")
      require(table != null, "Cassandra table name is required!")
      require(opName != null, "Operation is required!")

      val operation = Operations
        .find(_.name.equalsIgnoreCase(opName))
        .getOrElse(throw new IllegalArgumentException(s"Supported operations are: ${Operations.mkString(", ")}"))

      implicit lazy val spark: SparkSession = SparkSession
        .builder()
        .master(master)
        .config(ccc.ConnectionHostParam.name, host)
        .config(ccc.ConnectionPortParam.name, port)
        .getOrCreate()

      operation.perform(keyspace, table, numRows)
    }
  }

  private def printUsage(): Unit = {
    // Naively format help text assuming that the description is short enough.
    val optLen = OptDescriptions.map(_._1.length).max + 2
    OptDescriptions
      .map(t => t._1 + (" " * (optLen - t._1.length)) + t._2)
      .mkString(
        "",
        Separator,
        s"""
           |Examples:
           |  Add the following arguments to the spark-submit command as the application arguments.
           |  > Read
           |    --operation read \
           |    --cassandra-host cassandra.yourcompany.com \
           |    --keyspace your_keyspace --table your_table \
           |    --num-rows 20
           |  > Write
           |    --operation write \
           |    --cassandra-host cassandra.yourcompany.com \
           |    --keyspace your_keyspace --table your_table \
           |    --num-rows 100""".stripMargin
      )
  }

  private sealed trait Operation {

    def perform(keyspace: String, table: String, numRows: Int)(implicit spark: SparkSession): Unit

    final def name: String = toString
  }

  private case object Read extends Operation {

    override def perform(keyspace: String, table: String, numRows: Int)(implicit spark: SparkSession): Unit = {
      val df = spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(Map("keyspace" -> keyspace, "table" -> table))
        .load()

      df.show(numRows, truncate = false)
    }
  }

  private case object Write extends Operation {

    override def perform(keyspace: String, table: String, numRows: Int)(implicit spark: SparkSession): Unit = {
      import com.datastax.spark.connector._
      import org.apache.spark.sql.cassandra._
      import spark.implicits._

      val df = spark.createDataset(generateRows(numRows)).toDF()

      df.createCassandraTable(
        keyspace,
        table,
        partitionKeyColumns = Some(DummySchema.PartitionKeyColumns),
        clusteringKeyColumns = Some(DummySchema.ClusteringKeyColumns))

      df.write
        .cassandraFormat(table, keyspace)
        .save()
    }

    private def generateRows(numRows: Int): Seq[DummySchema] = {
      val rand = ThreadLocalRandom.current()

      def randomDate(): js.Date = {
        js.Date.valueOf(LocalDate.now().minusDays(rand.nextLong(0L, 200L)))
      }

      def randomTimestamp(): js.Timestamp = {
        js.Timestamp.valueOf(LocalDateTime.now().minusDays(rand.nextLong(0L, 200L)))
      }

      (1 to numRows)
        .map(
          i =>
            DummySchema(
              s"row_$i",
              rand.nextBoolean(),
              i,
              rand.nextLong(),
              rand.nextFloat(),
              rand.nextDouble(),
              randomDate(),
              randomTimestamp()))
    }
  }
}
