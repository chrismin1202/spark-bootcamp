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

from pyspark.sql.types import DoubleType
from pyspark.sql.types import StructType, StructField

from src.spark_ml_linear_regression import evaluate_model, fit_linear_regression, vectorize_and_scale_df
from test.spark_test_case import SparkTestCase


class TestLinearRegressionExample(SparkTestCase):

    def test_linear_regression_example(self):
        """Runs linear_regression_example.

        You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
        where your Python3 is installed.
        If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
        but it depends on how you installed Python3.
        """

        schema = StructType([
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("housingMedianAge", DoubleType()),
            StructField("totalRooms", DoubleType()),
            StructField("totalBedRooms", DoubleType()),
            StructField("population", DoubleType()),
            StructField("households", DoubleType()),
            StructField("medianIncome", DoubleType()),
            StructField("medianHouseValue", DoubleType())
        ])

        # ../resources/cal_housing.data is the local file path
        # If the SparkSession is created with HDFS access,
        # you need to either upload the file cal_housing.data to HDFS or
        # use fully qualified path with 'file://'
        df = self.spark.read \
            .option("inferSchema", value=False) \
            .option("header", value=False) \
            .schema(schema) \
            .csv("../resources/cal_housing.data")

        # Transform the DataFrame
        scaled_df = vectorize_and_scale_df(df)

        # Split the data into train and test sets
        train_df, test_df = scaled_df.randomSplit([0.8, 0.2], seed=12347)

        linear_model = fit_linear_regression(train_df)

        # Generate predictions
        predicted_df = linear_model.transform(test_df)

        # Print a few sample rows
        predicted_df \
            .select("prediction", "label") \
            .show(10, truncate=False)

        # Evaluate the model
        evaluate_model(linear_model)
