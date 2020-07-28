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

# SDSC2018-Spark-Bootcamp Code Examples

This repository contains code samples for Apache Spark/Spark MLlib Workshop @ [SDSC](https://www.southerndatascience.com/) 2018.

* Scala Version: 2.11.12 (You can downgrade, but I recommend 2.11.8 or higher)
* Recommended IDE: IntelliJ

Examples
--------
* [`Dataset` Join Examples](src/main/scala/chrism/sdsc/join)
* [Machine Learning](src/main/scala/chrism/sdsc/ml): A simple `NaiveBayes`-based spam detector
  * Note that the [dataset](src/main/resources/chrism/sdsc/ml/spam.csv) for training is in the resources folder.
* [Spark Streaming Example](src/main/scala/chrism/sdsc/streaming): A simple streaming job that counts the number of occurrences each word in a stream.
* [Task Not Serializable Example](src/main/scala/chrism/sdsc/tasknotserializable)
* More examples in Databricks Notebooks:
  * [Scala Examples](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120540240849139/117313189828149/7749756382321419/latest.html): Contains some Scala basics.
  * [`RDD` Examples](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120540240849139/1810795214606221/7749756382321419/latest.html)
  * [`DataFrame` Examples](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120540240849139/1810795214606226/7749756382321419/latest.html)
  * [`Dataset` Examples](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120540240849139/1810795214606231/7749756382321419/latest.html)
  * [Machine Learning](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4120540240849139/1810795214606255/7749756382321419/latest.html): Same as the machine learning example in this repository.
