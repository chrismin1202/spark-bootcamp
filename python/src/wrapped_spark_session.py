from pyspark.sql import SparkSession


class WrappedSparkSession:
    class __WrappedSparkSession:
        spark = SparkSession.builder \
            .master('local[*]') \
            .appName('Spark Demo') \
            .getOrCreate()

        def __del__(self):
            print('Stopping SparkSession...')
            self.spark.stop()
            self.spark = None

    instance = None

    def __init__(self):
        if not WrappedSparkSession.instance:
            print('Initializing SparkSession...')
            WrappedSparkSession.instance = WrappedSparkSession.__WrappedSparkSession()
        else:
            print('SparkSession has already been initialized.')

    @staticmethod
    def get_or_create():
        if not WrappedSparkSession.instance:
            WrappedSparkSession()
        return WrappedSparkSession.__WrappedSparkSession.spark
