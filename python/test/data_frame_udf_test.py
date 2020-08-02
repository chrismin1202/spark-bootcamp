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

from pyspark.sql import Row

from src.data_frame_udf import apply_udf_to_data_frame, apply_udf_to_sql
from test.spark_test_case import SparkTestCase


def _create_test_df(spark):
    return spark.createDataFrame(
        [Row(convert_me=0, do_not_mess_with_me=None),
         Row(convert_me=None, do_not_mess_with_me=1),
         Row(convert_me=1, do_not_mess_with_me=0)])


class TestDataFrameUdf(SparkTestCase):

    def test_applying_udf_to_data_frame(self):
        """Runs apply_udf_to_data_frame.

        You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
        where your Python3 is installed.
        If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
        but it depends on how you installed Python3.
        """
        df = _create_test_df(self.spark)
        converted_df = apply_udf_to_data_frame(df, "convert_me", "converted")
        self._assert_data_frame(converted_df)

    def test_applying_udf_to_sql(self):
        """Runs apply_udf_to_sql.

        You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
        where your Python3 is installed.
        If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
        but it depends on how you installed Python3.
        """
        df = _create_test_df(self.spark)
        converted_df = apply_udf_to_sql(self.spark, df, "convert_me", "converted")
        self._assert_data_frame(converted_df)

    def _assert_data_frame(self, df):
        row_tuples = set(map(lambda r: (r.converted, r.do_not_mess_with_me), df.collect()))
        self.assertEqual(len(row_tuples), 3)

        expected = {(False, None), (None, 1), (True, 0)}
        self.assertSetEqual(row_tuples, expected)
