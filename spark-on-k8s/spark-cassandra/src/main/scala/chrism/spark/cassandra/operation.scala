package chrism.spark.cassandra

import java.time.{LocalDate, LocalDateTime}
import java.util.concurrent.ThreadLocalRandom
import java.{sql => js}

import org.apache.spark.sql.SparkSession

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
