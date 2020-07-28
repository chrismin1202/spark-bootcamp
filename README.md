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

# Spark Workshop Code Examples

This repository contains code samples for Apache Spark/Spark MLlib Workshop @ [SDSC](https://www.southerndatascience.com/) 2019 Atlanta.

* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* Python Version: 3.x
* Recommended IDEs
  * IntelliJ for Scala examples
  * PyCharm for Python examples

Examples
--------
* [Scala SBT example](scala-sbt): Am example for building a deployable Spark application using SBT
* [Scala Maven example](scala-maven): An example for building a deployable Spark application using Maven.
  * Note that this example demonstrates how to structure multi-module projects.
* [Python example](python): A simple example for structuring a simple Spark application in Python.
* [Kubernetes example](spark-on-k8s): An example for running Spark on Kubernetes. Note that the example uses Cassandra as the persistent storage.
