#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.sql import SparkSession


# Do not use this class as is.
# There should be a better way to handle SparkSession instance in Python.
# I am no Python expert and I don't know what the pythonic approach for handling static resource is.
# Note that SparkSession is a singleton by design and in each JVM instance, there only exists one instance
# of SparkSession (hence, the builder's "build" method is named `getOrCreate` as opposed to `build`).
# Therefore, it is not a big deal to rebuild SparkSession for each Spark application as the same instance is reused
# if all Spark applications run on the same JVM instance.

class WrappedSparkSession:
    class __WrappedSparkSession:
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Spark Demo") \
            .getOrCreate()

        def __del__(self):
            print("Stopping SparkSession...")
            self.spark.stop()
            self.spark = None

    instance = None

    def __init__(self):
        if not WrappedSparkSession.instance:
            print("Initializing SparkSession...")
            WrappedSparkSession.instance = WrappedSparkSession.__WrappedSparkSession()
        else:
            print("SparkSession has already been initialized.")

    @staticmethod
    def get_or_create():
        if not WrappedSparkSession.instance:
            WrappedSparkSession()
        return WrappedSparkSession.__WrappedSparkSession.spark
