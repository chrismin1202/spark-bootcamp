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
