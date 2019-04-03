package chrism.spark.cassandra
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.ListMap

private final case class ParsedArgs(
  cassandraHost: String,
  keyspace: String,
  table: String,
  operation: Operation,
  cassandraPort: Short = ParsedArgs.DefaultPort,
  sparkMaster: String = ParsedArgs.DefaultMaster,
  numRows: Int = ParsedArgs.DefaultNumRows)

private object ParsedArgs extends LazyLogging {

  import com.datastax.spark.connector.cql.{CassandraConnectorConf => ccc}

  private val DefaultPort: Short = ccc.ConnectionPortParam.default.toShort
  private val DefaultMaster: String = "local"
  private val DefaultNumRows: Int = 10

  private[this] val Operations: Set[Operation] = Set(Read, Write)
  private[this] val HelpShortOpt: String = "-h"
  private[this] val HelpLongOpt: String = "--help"
  private[this] val SparkMasterLongOpt: String = "--spark-master"
  private[this] val CassandraHostLongOpt: String = "--cassandra-host"
  private[this] val CassandraPortLongOpt: String = "--cassandra-port"
  private[this] val TableLongOpt: String = "--table"
  private[this] val KeyspaceLongOpt: String = "--keyspace"
  private[this] val OperationLongOpt: String = "--operation"
  private[this] val NumRowsLongOpt: String = "--num-rows"
  // Using ListMap to keep the options in the order in which they are added.
  private[this] val OptDescriptions: ListMap[String, String] =
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

  private[this] lazy val Separator: String = System.lineSeparator()

  /** Parses the given commandline arguments and returns [[Some]] of parsed arguments.
    * [[None]] indicates that [[HelpShortOpt]] or [[HelpLongOpt]] is specified.
    * Note that if the arguments are not parsable, [[IllegalArgumentException]] can be thrown.
    *
    * @param args the commandline arguments to parse
    * @return the parsed arguments or [[None]] if [[HelpShortOpt]] or [[HelpLongOpt]] is specified
    * @throws IllegalArgumentException thrown if the arguments are not parsable
    */
  def parse(args: Array[String]): Option[ParsedArgs] =
    if (args.contains(HelpShortOpt) || args.contains(HelpLongOpt)) {
      logger.info("{} or {} specified. Returning None...", HelpShortOpt, HelpLongOpt)
      None
    } else {
      logger.info("Parsing arguments: {}", args: _*)

      // Naive argument parsing
      if (args.length % 2 != 0) {
        throw new IllegalArgumentException("The number of arguments must be an even number!")
      }

      var master: String = DefaultMaster
      var host: String = null
      var port: Short = DefaultPort
      var keyspace: String = null
      var table: String = null
      var opName: String = null
      var numRows: Int = DefaultNumRows
      args
        .grouped(2)
        .foreach({
          case Array(SparkMasterLongOpt, m) =>
            logger.info("Overriding Spark master {} with {}", master, m)
            master = m
          case Array(CassandraHostLongOpt, h) => host = h
          case Array(CassandraPortLongOpt, p) =>
            logger.info("Overriding Cassandra port {} with {}", port.toString, p)
            port = p.toShort
          case Array(KeyspaceLongOpt, ks)  => keyspace = ks
          case Array(TableLongOpt, t)      => table = t
          case Array(OperationLongOpt, op) => opName = op
          case Array(NumRowsLongOpt, n) =>
            logger.info("Overriding the default number of rows {} with {}", numRows.toString, n)
            numRows = n.toInt
        })

      require(host != null, "Cassandra host name is required!")
      require(keyspace != null, "Cassandra keyspace name is required!")
      require(table != null, "Cassandra table name is required!")
      require(opName != null, "Operation is required!")

      val operation = Operations
        .find(_.name.equalsIgnoreCase(opName))
        .getOrElse(throw new IllegalArgumentException(s"Supported operations are: ${Operations.mkString(", ")}"))

      Some(ParsedArgs(host, keyspace, table, operation, cassandraPort = port, sparkMaster = master, numRows = numRows))
    }

  def printUsage(): Unit = {
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
}
