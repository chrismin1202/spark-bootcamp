package chrism.sdsc.rdd2dataframe

import chrism.sdsc.Runner
import chrism.sdsc.model.WordFrequency
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object RDD2DataFrame extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    // stubbed
    // Run the unit test
  }

  def convertRDD2DataFrameManually(rdd: RDD[Int], schema: StructType)(implicit spark: SparkSession): DataFrame = {
    // Just making sure that the schema contains only 1 column
    assert(schema.size == 1, "Expecting 1 column in the schema!")
    val column = schema.head
    assert(
      column.dataType == DataTypes.IntegerType,
      "Expecting the column `" + column.name + "` to be of type " + DataTypes.IntegerType.sql +
        ", but it is " + column.dataType + "!"
    )

    // Convert each record in the given RDD to Row
    // Row's factory method takes in any number of element or any type, i.e., Any*
    val rows = rdd.map(Row(_))
    spark.sqlContext.createDataFrame(rows, schema)
  }

  def convertRDD2DataFrameImplicitly(rdd: RDD[Int])(implicit spark: SparkSession): DataFrame = {
    // Import the object `implicits`.
    // As its name suggests, it contains many implicit methods for working with DataFrame.
    import spark.sqlContext.implicits._

    // Note: If you know what the schema is going to be, you are better off resorting to the manual approach
    //       shown in `convertRdd2DataFrameManually` as implicit conversion requires reflection.

    // Convert each record in the given RDD to Row, but this time,
    // instead of defining schema object, we are going to have Spark implicitly (and reflectively)
    // infer the schema from the case class `Num`.
    rdd
      .map(Num) // Instead of converting to `Row` directly, convert each record to `Num` (defined below).
      .toDF() // Reflectively deduce the schema from the case class signature.
  }

  def convertRDDOfCaseClass2DataFrameImplicitly(rdd: RDD[WordFrequency])(implicit spark: SparkSession): DataFrame = {
    // Same as before.
    // Import the object `implicits`.
    import spark.sqlContext.implicits._

    // As opposed to `convertRdd2DataFrameImplicitly`, in this example, it is "acceptable" to
    // resort to implicit conversion as we are already working with case class.

    // Since we already have case class, we can call `toDF` without any further mapping.
    rdd.toDF()
  }
}

private final case class Num(number: Int)
