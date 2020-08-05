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

import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.FunSuite

final class ParsedArgsTest extends FunSuite with TypeCheckedTripleEquals {

  test("None is returned when -h or --help is specified") {
    assert(ParsedArgs.parse(Array("-h")).isEmpty)
    assert(ParsedArgs.parse(Array("--help")).isEmpty)
  }

  test("can parse arguments when only required arguments are specified") {
    val requiredArgs = Array("--operation", "read", "--cassandra-host", "hostname", "--keyspace", "ks", "--table", "t")

    val parsed = ParsedArgs.parse(requiredArgs).getOrElse(fail(s"Failed to parse ${requiredArgs.mkString(" ")}"))
    assert(parsed === ParsedArgs("hostname", "ks", "t", Read))
  }

  test("missing required arguments results in IllegalArgumentException") {
    // missing --operation
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--cassandra-host", "hostname", "--keyspace", "ks", "--table", "t"))
    }

    // missing --cassandra-host
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--keyspace", "ks", "--table", "t"))
    }

    // missing --keyspace
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--cassandra-host", "hostname", "--table", "t"))
    }

    // missing --table
    intercept[IllegalArgumentException] {
      ParsedArgs.parse(Array("--operation", "read", "--cassandra-host", "hostname", "--keyspace", "ks"))
    }
  }
}
