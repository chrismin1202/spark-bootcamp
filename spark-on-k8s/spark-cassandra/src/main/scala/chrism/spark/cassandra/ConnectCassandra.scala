/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
