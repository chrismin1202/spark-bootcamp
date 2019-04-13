package chrism.sdsc.dataframe

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class DefineYourOwnFunctionsTest extends TestSuite with TestSparkSessionMixin {

  import DefineYourOwnFunctionsTest.SchemaEncoder

  test("applying UDF to DataFrame API") {
    val df = createDataFrame()
    val convertedDF =
      DefineYourOwnFunctions.applyUDFDataFrameAPI(df, "convert_me", convertedColumnName = Some("converted"))
    val rows = convertedDF.collect()
    rows should have length 3

    rows.foreach { r =>
      // Make sure that `convert_me` column has been replaced with `converted`.
      intercept[IllegalArgumentException] {
        r.fieldIndex("convert_me")
      }
    }
    // The column `converted` should be at the index at which `convert_me` used to be.
    assert(rows.forall(_.fieldIndex("converted") == 0))

    // Map each row as Tuple2 (Option[Boolean], Option[Int])
    rows.map(r => (r.getBooleanOrNone(0), r.getIntOrNone(1))) should contain theSameElementsAs Seq(
      (Some(false), None),
      (None, Some(1)),
      (Some(true), Some(0)))
  }

  test("applying UDF to SQL expression") {
    val df = createDataFrame()
    val convertedDF =
      DefineYourOwnFunctions.applyUDFSQLStyle(df, "convert_me", convertedColumnName = Some("converted"))

    // Same as before, apply the same tests.
    val rows = convertedDF.collect()
    rows should have length 3

    rows.foreach { r =>
      // Make sure that `convert_me` column has been replaced with `converted`.
      intercept[IllegalArgumentException] {
        r.fieldIndex("convert_me")
      }
    }
    // The column `converted` should be at the index at which `convert_me` used to be.
    assert(rows.forall(_.fieldIndex("converted") == 0))

    // Map each row as Tuple2 (Option[Boolean], Option[Int])
    rows.map(r => (r.getBooleanOrNone(0), r.getIntOrNone(1))) should contain theSameElementsAs Seq(
      (Some(false), None),
      (None, Some(1)),
      (Some(true), Some(0)))
  }

  /** Generates a [[DataFrame]] with 3 rows:
    *   +----------+-------------------+
    *   |convert_me|do_not_mess_with_me|
    *   +----------+-------------------+
    *   |         0|               NULL|
    *   |      NULL|                  1|
    *   |         1|                  0|
    *   +----------+-------------------+
    *
    * @return a [[DataFrame]] with test data
    */
  private def createDataFrame(): DataFrame =
    spark.createDataset(Seq(Row(0, null), Row(null, 1), Row(1, 0)))(SchemaEncoder)

  private implicit final class RowOps(row: Row) {

    def getBooleanOrNone(i: Int): Option[Boolean] = if (row.isNullAt(i)) None else Some(row.getBoolean(i))

    def getIntOrNone(i: Int): Option[Int] = if (row.isNullAt(i)) None else Some(row.getInt(i))
  }
}

private[this] object DefineYourOwnFunctionsTest {

  private val Schema = StructType(
    Seq(
      // an INT column to convert by applying the UDF
      StructField("convert_me", DataTypes.IntegerType),
      // another column to pass through
      StructField("do_not_mess_with_me", DataTypes.IntegerType)
    ))

  private val SchemaEncoder: ExpressionEncoder[Row] = RowEncoder(Schema)
}
