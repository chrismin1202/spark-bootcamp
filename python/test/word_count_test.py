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

from src.word_count import count_words
from test.spark_test_case import SparkTestCase


class TestWordCount(SparkTestCase):

    def test_word_count(self):
        """Runs word_count.

        You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
        where your Python3 is installed.
        If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
        but it depends on how you installed Python3.
        """

        blob = """
        The path of the righteous man is beset on all sides by the inequities of the selfish and the tyranny 
        of evil men. Blessed is he, who in the name of charity and good will, 
        shepherds the weak through the valley of darkness, 
        for he is truly his brother's keeper and the finder of lost children. 
        And I will strike down upon thee with great vengeance and furious anger those who would attempt to poison 
        and destroy my brothers. And you will know my name is the Lord when I lay my vengeance upon thee."""

        df = count_words(blob, self.spark) \
            .orderBy("frequency", ascending=False)

        df.show(20, truncate=False)

        first_row = df.first()
        self.assertEqual(first_row.word, "the")
        self.assertEqual(first_row.frequency, 10)
