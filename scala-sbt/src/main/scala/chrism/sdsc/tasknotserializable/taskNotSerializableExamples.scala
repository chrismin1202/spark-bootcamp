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

import chrism.sdsc.Runner
import org.apache.spark.sql.{Dataset, SparkSession}

object NotSerializableVersion extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // If you run this example, you will get "org.apache.spark.SparkException Task not serializable".
    new NotSerializableJob("number")
      .prepend((1 to 10).toDS())
      .show(10, truncate = false)
  }

  // If you need to use class over object for some reason...
  private final class NotSerializableJob(prefix: String) {

    def prepend(numDs: Dataset[Int])(implicit spark: SparkSession): Dataset[String] = {
      import spark.implicits._

      // The task is not serializable because the closure (num => value + num) depends on the state of the class.
      numDs.map(num => prefix + num)
    }
  }
}

object SerializableVersion extends Runner {

  override def run(args: Array[String])(implicit spark: SparkSession): Unit = {
    import spark.implicits._

    // If you run this example, you will not get "org.apache.spark.SparkException Task not serializable".
    // It is rather simple to get around java.io.NotSerializableException,
    // which is the cause exception of org.apache.spark.SparkException thrown by NotSerializableVersion.
    // You just need to isolate the executor from the driver code.
    new SerializableJob("number")
      .prepend((1 to 10).toDS())
      .show(10, truncate = false)
  }

  // Decouple the code that is run by the executors from the driver code
  private final class SerializableJob(prefix: String) {

    def prepend(numDs: Dataset[Int])(implicit spark: SparkSession): Dataset[String] = prependHelper(numDs, prefix)

    // By passing in prefix as a parameter to the method that is run by the executors,
    // this method no longer depends on the state of the class.
    private def prependHelper(numDs: Dataset[Int], prefix: String)(implicit spark: SparkSession): Dataset[String] = {
      import spark.implicits._

      // The task is now serializable because the closure (num => value + num)
      // no longer depends on the state of the class.
      numDs.map(num => prefix + num)
    }
  }
}
