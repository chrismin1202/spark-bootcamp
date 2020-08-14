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

package chrism.sdsc.join

import chrism.sdsc.{TestSparkSessionMixin, TestSuite}

final class JoinExamplesTest extends TestSuite with TestSparkSessionMixin {

  test("INNER JOIN") {
    val profiles = JoinExamples.joinExample().collect()
    val expected = Seq(
      NullSafeProfile(1, first = Some("Rachel"), last = Some("Green"), gender = Some("female")),
      NullSafeProfile(2, first = Some("Monica"), last = Some("Geller"), gender = Some("female")),
      NullSafeProfile(3, first = Some("Phoebe"), last = Some("Buffey"), gender = Some("female")),
      NullSafeProfile(4, first = Some("Joey"), last = Some("Tribbiani"), gender = Some("male")),
      NullSafeProfile(5, first = Some("Chandler"), last = Some("Bing"), gender = Some("male")),
      NullSafeProfile(6, first = Some("Ross"), last = Some("Geller"), gender = Some("male")),
    )
    profiles should contain theSameElementsAs expected
  }

  test("INNER JOIN SQL version") {
    JoinExamples.joinExampleSql().show(10, truncate = false)
  }

  test("INNER JOIN DataFrame version") {
    JoinExamples.joinExampleDF().show(10, truncate = false)
  }

  test("LEFT OUTER JOIN") {
    val profiles = JoinExamples.leftOuterJoinExample().collect()
    val expected = Seq(
      Profile(1, first = "Rachel", last = "Green", gender = "female", jobTitle = "Waitress"),
      Profile(2, first = "Monica", last = "Geller", gender = "female", jobTitle = "Chef"),
      Profile(3, first = "Phoebe", last = "Buffey", gender = "female", jobTitle = "Masseuse"),
      Profile(4, first = "Joey", last = "Tribbiani", gender = "male", jobTitle = "Actor"),
      Profile(5, first = "Chandler", last = "Bing", gender = "male"),
      Profile(6, first = "Ross", last = "Geller", gender = "male", jobTitle = "Paleontologist"),
    )
    profiles should contain theSameElementsAs expected
  }

  test("LEFT OUTER JOIN alternative") {
    val profiles = JoinExamples.leftOuterJoin2Example().collect()
    val expected = Seq(
      Profile(1, first = "Rachel", last = "Green", gender = "female", jobTitle = "Waitress"),
      Profile(2, first = "Monica", last = "Geller", gender = "female", jobTitle = "Chef"),
      Profile(3, first = "Phoebe", last = "Buffey", gender = "female", jobTitle = "Masseuse"),
      Profile(4, first = "Joey", last = "Tribbiani", gender = "male", jobTitle = "Actor"),
      Profile(5, first = "Chandler", last = "Bing", gender = "male"),
      Profile(6, first = "Ross", last = "Geller", gender = "male", jobTitle = "Paleontologist"),
    )
    profiles should contain theSameElementsAs expected
  }
}
