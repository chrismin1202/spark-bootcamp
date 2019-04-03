package chrism.spark.cassandra

import java.{sql => js}

final case class DummySchema(
  s: String,
  b: Boolean,
  i32: Int,
  l64: Long,
  f32: Float,
  d64: Double,
  d: js.Date,
  ts: js.Timestamp)

object DummySchema {

  val PartitionKeyColumns: Seq[String] = Seq("i32", "b")
  val ClusteringKeyColumns: Seq[String] = Seq("ts", "d")
}
