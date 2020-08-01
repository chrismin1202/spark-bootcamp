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

package chrism.sdsc.comparison

import chrism.sdsc.Runner
import chrism.sdsc.model.WordFrequency
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object PhysicalPlans extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    // stubbed
    // Run the unit test
  }

  def dataFrameGroupByThenSum(ds: Dataset[_])(implicit spark: SparkSession): DataFrame = {
    val schema = ds.schema
    require(
      schema.exists(c => c.name == "word" && c.dataType == DataTypes.StringType),
      "There is no column named `word` of type STRING!")
    require(
      schema.exists(c => c.name == "frequency" && c.dataType == DataTypes.LongType),
      "There is no column named `frequency` of type BIGINT!")

    runAndMeasure {
      ds.groupBy("word")
        .sum("frequency")
    }
  }

  def datasetMapGroups(ds: Dataset[WordFrequency])(implicit spark: SparkSession): Dataset[WordFrequency] = {
    import spark.implicits._

    runAndMeasure {
      ds.groupByKey(_.word)
        .mapGroups((word, iterator) => WordFrequency(word, iterator.map(_.frequency).sum))
    }
  }

  def datasetReduceGroups(ds: Dataset[WordFrequency])(implicit spark: SparkSession): Dataset[WordFrequency] = {
    import spark.implicits._

    runAndMeasure {
      ds.groupByKey(_.word)
        .reduceGroups(_ + _)
        .map(_._2)
    }
  }

  private def runAndMeasure[R](func: => R): R = {
    val start = System.currentTimeMillis()
    val ret = func
    val end = System.currentTimeMillis()
    println(s"Duration: ${end - start}ms")
    ret
  }
}
