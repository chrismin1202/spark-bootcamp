package chrism.spark.cassandra

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object ConnectCassandra extends LazyLogging {

  import com.datastax.spark.connector.cql.{CassandraConnectorConf => ccc}

  def main(args: Array[String]): Unit =
    ParsedArgs.parse(args) match {
      case Some(pa) =>
        logger.info(
          "Initializing SparkSession with {} as Spark master, {} as Cassandra host, and {} as Cassandra port...",
          pa.sparkMaster,
          pa.cassandraHost,
          pa.cassandraPort.toString
        )
        implicit val spark: SparkSession = SparkSession
          .builder()
          .master(pa.sparkMaster)
          .config(ccc.ConnectionHostParam.name, pa.cassandraHost)
          .config(ccc.ConnectionPortParam.name, pa.cassandraPort)
          .getOrCreate()

        logger.info("keyspace={}, table={}, numRows={}", pa.keyspace, pa.table, pa.numRows.toString)
        pa.operation.perform(pa.keyspace, pa.table, pa.numRows)
      case _ => ParsedArgs.printUsage()
    }
}
