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

package chrism.sdsc.rdd2dataframe

import chrism.sdsc.model.WordFrequency
import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import scala.util.matching.Regex

final class RDD2DataFrameTest extends TestSuite with TestSparkSessionMixin {

  test("converting RDD of primitive type to DataFrame manually") {
    // Let's pretend that there exists an RDD that you want to convert to DataFrame.
    val numberRDD = spark.sparkContext.parallelize(1 to 10).cache() // RDD[Int]

    // Define the schema of the DataFrame by creating StructType.
    val numberColumn = StructField("number", DataTypes.IntegerType)
    val schema = StructType(Seq(numberColumn))

    val df = RDD2DataFrame.convertRDD2DataFrameManually(numberRDD, schema) // DataFrame

    // Collect the rows in the DataFrame to the driver
    val rows = df.collect() // Array[Row]
    // Since we generated 10 numbers, there should be 10 rows.
    rows should have length 10
    // Since there is only 1 column in each row, the size of all rows should be 1.
    assert(rows.forall(_.size == 1))
    // Since there is only 1 column in each row, the index of the column should be 0.
    assert(rows.forall(_.fieldIndex(numberColumn.name) == 0))
    // Now we know that all rows have a column named `number` at index 0, let's check the contents.
    rows.map(_.getInt(0)) should contain theSameElementsAs (1 to 10)
  }

  test("converting RDD of primitive type to DataFrame implicitly") {
    // Once again, let's pretend that there exists an RDD that you want to convert to DataFrame.
    val numberRDD = spark.sparkContext.parallelize(1 to 10).cache() // RDD[Int]

    val df = RDD2DataFrame.convertRDD2DataFrameImplicitly(numberRDD)

    // Collect the rows in the DataFrame to the driver
    val rows = df.collect()
    // Since we generated 10 numbers, there should be 10 rows.
    rows should have length 10
    // Since there is only 1 column in each row, the size of all rows should be 1.
    assert(rows.forall(_.size == 1))
    // Since there is only 1 column in each row, the index of the column `number`,
    // which is inferred from `Num` defined in rdd2DataFrame.scala, should be 0.
    assert(rows.forall(_.fieldIndex("number") == 0))
    // Now we know that all rows have a column named `number` at index 0, let's check the contents.
    rows.map(_.getInt(0)) should contain theSameElementsAs (1 to 10)
  }

  test("converting RDD of case class to DataFrame implicitly") {
    import RDD2DataFrameTest.SplitRegex
    import spark.implicits._

    val text =
      "The path of the righteous man is beset on all sides by the inequities of the selfish " +
        "and the tyranny of evil men. " +
        "Blessed is he who, in the name of charity and good will, " +
        "shepherds the weak through the valley of darkness, " +
        "for he is truly his brother's keeper and the finder of lost children. " +
        "And I will strike down upon thee with great vengeance and furious anger " +
        "those who attempt to poison and destroy my brothers. " +
        "And you will know my name is the Lord when I lay my vengeance upon thee."

    val rdd = spark.sparkContext
      .parallelize(Seq(text)) // RDD[String]
      .flatMap(SplitRegex.split) // RDD[String]
      .map(_.toLowerCase) // RDD[String]
      .map(WordFrequency(_)) // RDD[WordFrequency]

    val df = RDD2DataFrame.convertRDDOfCaseClass2DataFrameImplicitly(rdd)

    // Use DataFrame API to collect top 5 most frequently occurring words.
    val rows = df
      .groupBy(functions.col("word")) // GROUP BY `word`
      .sum("frequency") // sum(`frequency`)
      .withColumnRenamed("sum(frequency)", "frequency") // sum(`frequency`) AS `frequency`
      .withColumn(
        "frequency_rank",
        functions.dense_rank().over(Window.orderBy($"frequency".desc))
      ) // dense_rank() OVER (ORDER BY `frequency` DESC) AS `frequency_rank`
      .where($"frequency_rank" <= 5) // WHERE `frequency_rank` <= 5
      .orderBy($"frequency".desc, $"word") // ORDER BY `frequency` DESC, `word`
      .select($"word", $"frequency")
      .collect()
    // Let's make sure that 5 rows have been collected.
    rows should have length 6
    // Let's check the schema.
    // The first and second columns should be `word` of type STRING and `frequency` of type BIGINT respectively.
    assert(rows.forall(_.fieldIndex("word") == 0))
    assert(rows.forall(_.fieldIndex("frequency") == 1))

    // The top 5 words are:
    //   +-----+---------+
    //   | word|frequency|
    //   +-----+---------+
    //   |  the|       10|
    //   |  and|        7|
    //   |   of|        6|
    //   |   is|        4|
    //   |   my|        3|
    //   | will|        3|
    //   +-----+---------+
    // Let's make sure that the 5 rows match the expectation.
    rows.map(r => WordFrequency(r.getString(0), r.getLong(1))) should contain theSameElementsInOrderAs Seq(
      WordFrequency("the", frequency = 10L),
      WordFrequency("and", frequency = 7L),
      WordFrequency("of", frequency = 6L),
      WordFrequency("is", frequency = 4L),
      WordFrequency("my", frequency = 3L),
      WordFrequency("will", frequency = 3L),
    )
  }
}

private[this] object RDD2DataFrameTest {

  private val SplitRegex: Regex = "[\\s.,]+".r // .r compiles the string literal into scala.util.matching.Regex
}
