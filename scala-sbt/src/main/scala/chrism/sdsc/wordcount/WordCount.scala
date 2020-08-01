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

package chrism.sdsc.wordcount

import java.util.concurrent.ThreadLocalRandom

import chrism.sdsc.Runner
import chrism.sdsc.model.WordFrequency
import org.apache.spark.sql.{functions, SparkSession}

object WordCount extends Runner {

  private val AsciiLowercaseA: Int = 97
  private val NumLetters: Int = 26

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    val nums = 1 to 4
    spark
      .createDataset(nums)
      .repartition(nums.size)
      .flatMap(_ => generateRandomWords())
      .groupByKey(_.word)
      .reduceGroups(_ + _)
      .map(_._2)
      .orderBy(functions.col("frequency").desc)
      .limit(NumLetters)
      .show(NumLetters, truncate = false)
  }

  /** Generates 10,000 random words.
    * Note that the "words" are one of the lowercased English alphabet: a-z.
    *
    * @return 10,000 randomly generated words
    */
  private def generateRandomWords(/* potential IO */ ): Seq[WordFrequency] = {
    val rand = ThreadLocalRandom.current()
    (1 to 10000)
      .map(_ => rand.nextInt(AsciiLowercaseA, AsciiLowercaseA + NumLetters))
      .map(_.asInstanceOf[Char].toString)
      .map(WordFrequency(_))
  }
}
