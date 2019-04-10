from pyspark.ml.feature import StandardScaler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.regression import LinearRegression
from pyspark.sql import functions


# Note that I found this example from freely available DataCamp Community Tutorials.
# The tutorial was written in 2017 and I noticed that it was using RDD.
# I'm just updating the example to demonstrate that it can be rewritten without using RDD directly.
# Credit: https://www.datacamp.com/community/tutorials/apache-spark-tutorial-machine-learning

def vectorize_and_scale_df(df):
    # Transform some columns and select only the columns needed for the subsequent step.
    # Note that if the column name already exists, `withColumn` replaces the column.
    transformed_df = df \
        .withColumn('medianHouseValue', functions.col('medianHouseValue') / 100000) \
        .withColumn('roomsPerHousehold', functions.col('totalRooms') / functions.col('households')) \
        .withColumn('populationPerHousehold', functions.col('population') / functions.col('households')) \
        .withColumn('bedroomsPerRoom', functions.col('totalBedRooms') / functions.col('totalRooms')) \
        .select(
            'medianHouseValue',
            'totalBedRooms',
            'population',
            'households',
            'medianIncome',
            'roomsPerHousehold',
            'populationPerHousehold',
            'bedroomsPerRoom'
        )

    vectorize_dense = functions.udf(lambda r: Vectors.dense(r), VectorUDT())
    vectorized_df = transformed_df \
        .select(
            functions.col('medianHouseValue').alias('label'),
            vectorize_dense(
                functions.array(
                    'totalBedRooms',
                    'population',
                    'households',
                    'medianIncome',
                    'roomsPerHousehold',
                    'populationPerHousehold',
                    'bedroomsPerRoom'
                ))
            .alias('features')
        )

    # Initialize the StandardScaler.
    standard_scaler = StandardScaler(inputCol='features', outputCol='features_scaled')

    # Fit the DataFrame to the scaler.
    scaler = standard_scaler.fit(vectorized_df)

    # Transform the data in `df` with the scaler
    return scaler.transform(vectorized_df)


def fit_linear_regression(train_df):
    # Initialize LinearRegression
    linear_regression = LinearRegression(labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the data to the model
    return linear_regression.fit(train_df)


def evaluate_model(linear_model):
    print(
        'Coefficients: {0}\nIntercept: {1:0.8f}\nRMSE: {2:0.8f}\nR^2: {3:0.8f}'.format(
            # Coefficients for the model
            str(linear_model.coefficients),
            # Intercept for the model
            linear_model.intercept,
            # Root Mean Squared Error
            linear_model.summary.rootMeanSquaredError,
            # R^2
            linear_model.summary.r2)
    )
