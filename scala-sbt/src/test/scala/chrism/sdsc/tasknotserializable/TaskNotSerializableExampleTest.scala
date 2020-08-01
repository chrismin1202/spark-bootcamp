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

package chrism.sdsc.tasknotserializable

import java.io.NotSerializableException

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}
import org.apache.spark.SparkException

final class TaskNotSerializableExampleTest extends TestSuite with TestSparkSessionMixin {

  test("Task not serializable example") {
    val sparkException =
      intercept[SparkException] {
        NotSerializableVersion.run(Array.empty)
      }
    assert(sparkException.getMessage.contains("Task not serializable"))
    sparkException.getCause match {
      case ex: NotSerializableException => println(ex.getMessage)
      case other                        => fail(s"$other is not an expected cause!")
    }
  }

  test("Serializable job") {
    // should print number1 - number10
    SerializableVersion.run(Array.empty)
  }
}
