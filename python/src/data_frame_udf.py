import random
import sys

from pyspark.sql import functions
from pyspark.sql.types import BooleanType


def to_boolean(num):
    if num == 0:
        return False
    elif num == 1:
        return True
    else:
        return None


def convert_column(column_name, num_column_name, converted_column_name, udf):
    if column_name == num_column_name:
        return udf(functions.col(column_name)).alias(converted_column_name)
    else:
        return functions.col(column_name)


def apply_udf_to_data_frame(df, num_column, converted_column):
    to_boolean_udf = functions.udf(to_boolean, BooleanType())

    return df.select(
        *[to_boolean_udf(column).alias(converted_column) if column == num_column else column for column in df.columns])


def apply_udf_to_sql(spark, df, num_column, converted_column):
    # Register the UDF to SparkSession
    spark.udf.register('to_boolean', to_boolean, "BOOLEAN")

    view = 'temp_table_' + str(random.randint(0, sys.maxsize))
    df.createOrReplaceTempView(view)

    column_expr = ', '.join([
        'to_boolean(`{0}`) AS `{1}`'.format(column, converted_column) if column == num_column
        else '`{0}`'.format(column)
        for column in df.columns])
    return spark.sql('SELECT {0} FROM `{1}`'.format(column_expr, view))
