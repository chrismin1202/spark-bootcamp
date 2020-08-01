<!---
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

## Multi-module Spark application example in Scala + Maven

* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* Maven Version: 3.x
* Recommended IDE: IntelliJ

Examples
--------
* [Spark Unit Testing Setup](common/src/test/scala/chrism/sdsc): An example of how to setup unit-test base classes for Spark and Hadoop
  * [TestSuite.scala](common/src/test/scala/chrism/sdsc/TestSuite.scala): a Scalatest base class example
  * [TestSparkSessionMixin.scala](common/src/test/scala/chrism/sdsc/spark/TestSparkSessionMixin.scala): a mixin `trait` for handling `SparkSession`
  * [TestHadoopFileSystemMixin.scala](common/src/test/scala/chrism/sdsc/hadoop/TestHadoopFileSystemMixin.scala): a mixin `trait` for simulating Hadoop `FileSystem` in unit-testing environment
  * [TestSuite](common/src/test/scala/chrism/sdsc/TestSuite.scala): a Scalatest base class example
* [Spark CSV Read Examples](common/src/test/scala/chrism/sdsc/spark/csv): An example of how to setup unit-test base classes for Spark and Hadoop
* [Machine Learning](ml-example/src/main/scala/chrism/sdsc/ml): A simple `NaiveBayes`-based spam detector
  * Note that the [dataset](ml-example/src/main/resources/chrism/sdsc/ml/spam.csv) for training is in the resources folder.
* [Spark Streaming Example](streaming-example/src/main/scala/chrism/sdsc/streaming): A simple streaming job that counts the number of occurrences each word in a stream.
