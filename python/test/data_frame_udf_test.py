from pyspark.sql import Row

from src.data_frame_udf import apply_udf_to_data_frame, apply_udf_to_sql
from src.wrapped_spark_session import WrappedSparkSession


def test_applying_udf_to_data_frame():
    """Runs apply_udf_to_data_frame.

    You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
    where your Python3 is installed.
    If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
    but it depends on how you installed Python3.
    """

    spark = WrappedSparkSession.get_or_create()

    df = spark.createDataFrame(
        [Row(convert_me=0, do_not_mess_with_me=None),
         Row(convert_me=None, do_not_mess_with_me=1),
         Row(convert_me=1, do_not_mess_with_me=0)])
    converted_df = apply_udf_to_data_frame(df, 'convert_me', 'converted')

    row_tuples = list(map(lambda r: (r.converted, r.do_not_mess_with_me), converted_df.collect()))
    assert len(row_tuples) == 3
    assert (False, None) in row_tuples
    assert (None, 1) in row_tuples
    assert (True, 0) in row_tuples


def test_applying_udf_to_sql():
    """Runs apply_udf_to_sql.

    You may need to have the environment variables PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON point to
    where your Python3 is installed.
    If you are on Unix-like system, it's likely to be /usr/bin/python3 or /usr/bin/local/python3,
    but it depends on how you installed Python3.
    """

    spark = WrappedSparkSession.get_or_create()

    df = spark.createDataFrame(
        [Row(convert_me=0, do_not_mess_with_me=None),
         Row(convert_me=None, do_not_mess_with_me=1),
         Row(convert_me=1, do_not_mess_with_me=0)])
    converted_df = apply_udf_to_sql(spark, df, 'convert_me', 'converted')

    row_tuples = list(map(lambda r: (r.converted, r.do_not_mess_with_me), converted_df.collect()))
    assert len(row_tuples) == 3
    assert (False, None) in row_tuples
    assert (None, 1) in row_tuples
    assert (True, 0) in row_tuples
