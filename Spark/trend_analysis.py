import pyspark.pandas as ps_pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

from cassandra.cluster import Cluster

# Create cluster
# cluster = Cluster(['localhost'], port=9042)
cluster = Cluster(['cassandra-1', 'cassandra-2', 'cassandra-3'], port=9042)

session = cluster.connect()

session.set_keyspace("vietnam_stock")

spark = SparkSession.builder.appName("Linear Regression model building application").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


def pandas_factory(colnames, rows):
    """
        Function to get pyspark pandas dataframe
        from a query in Cassandra table
    """
    return ps_pd.DataFrame(rows, columns=colnames)


session.row_factory = pandas_factory

session.default_fetch_size = 10000000


def build_one_minute_linear_regression_model(training_data):
    one_min_lr = LinearRegression(featuresCol="features", labelCol="next_one_minute_price")

    one_min_model = one_min_lr.fit(training_data)

    return one_min_model


def one_minute_prediction(one_min_model, testing_data):
    one_min_prediction = one_min_model.transform(testing_data)

    return one_min_prediction


def evaluate_one_minute_linear_regression_model(one_min_prediction):
    """
        Evaluate the model for predicting stock index in next one minute
    """
    one_min_evaluator_r2 = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction",
                                               metricName="r2")

    one_min_r2 = one_min_evaluator_r2.evaluate(one_min_prediction)

    one_min_evaluator_rmse = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction",
                                                 metricName="rmse")

    one_min_rmse = one_min_evaluator_rmse.evaluate(one_min_prediction)

    one_min_evaluator_mae = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction",
                                                metricName="mae")

    one_min_mae = one_min_evaluator_mae.evaluate(one_min_prediction)

    return [one_min_r2, one_min_rmse, one_min_mae]


def build_five_minutes_linear_regression_model(training_data):
    five_mins_lr = LinearRegression(featuresCol="features", labelCol="next_five_minutes_price")

    five_mins_model = five_mins_lr.fit(training_data)

    return five_mins_model


def five_minutes_prediction(five_mins_model, testing_data):
    five_mins_prediction = five_mins_model.transform(testing_data)

    return five_mins_prediction


def evaluate_five_minutes_linear_regression_model(five_mins_prediction):
    """
        Evaluate the model for predicting stock index in next one minute
    """
    five_mins_evaluator_r2 = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction",
                                                 metricName="r2")

    five_mins_r2 = five_mins_evaluator_r2.evaluate(five_mins_prediction)

    five_mins_evaluator_rmse = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction",
                                                   metricName="rmse")

    five_mins_rmse = five_mins_evaluator_rmse.evaluate(five_mins_prediction)

    five_mins_evaluator_mae = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction",
                                                  metricName="mae")

    five_mins_mae = five_mins_evaluator_mae.evaluate(five_mins_prediction)

    return [five_mins_r2, five_mins_rmse, five_mins_mae]


def main():
    dataset = session.execute("""
        SELECT * FROM stock_data_for_ml;
    """)

    ps_df = dataset._current_rows

    ps_df['trading_date'] = ps_df['trading_time'].dt.date

    ps_df['time'] = ps_df['trading_time'].dt.strftime('%H:%M:%S').astype('str')

    stock_data = ps_df[
        ['trading_date', 'time', 'ticker', 'price', 'volume',
         'last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
         'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
         'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
         'last_five_minutes_volume', 'next_one_minute_price', 'next_five_minutes_price'
         ]
    ]

    stock_data = stock_data.to_spark()

    # Preprocess timestamp features
    stock_data = stock_data.withColumn("hour", hour(col("time"))) \
        .withColumn("minute", minute(col("time")))

    # Create StringIndexer for column 'ticker'
    string_indexer = StringIndexer(inputCol="ticker", outputCol="ticker_index")

    stock_data = string_indexer.fit(stock_data).transform(stock_data)

    onehot_encoder = OneHotEncoder(inputCols=["ticker_index"], outputCols=["ticker_encoded"])

    stock_data = onehot_encoder.fit(stock_data).transform(stock_data)

    assembler = VectorAssembler(inputCols=[
        'hour', 'minute', 'price', 'ticker_encoded', 'volume',
        'last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
        'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
        'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
        'last_five_minutes_volume'],
        outputCol='features')

    stock_transformed_data = assembler.transform(stock_data)

    (training_data, testing_data) = stock_transformed_data.randomSplit([0.8, 0.2], seed=42)

    one_min_model = build_one_minute_linear_regression_model(training_data)

    five_mins_model = build_five_minutes_linear_regression_model(training_data)

    one_min_prediction = one_minute_prediction(one_min_model, testing_data)

    five_mins_prediction = five_minutes_prediction(five_mins_model, testing_data)

    one_min_model_evaluation = evaluate_one_minute_linear_regression_model(one_min_prediction)

    five_mins_model_evaluation = evaluate_five_minutes_linear_regression_model(five_mins_prediction)

    data = [{'model': 'next one minute price prediction',
             'r2': one_min_model_evaluation[0],
             'rmse': one_min_model_evaluation[1],
             'mae': one_min_model_evaluation[2]},
            {'model': 'next five minutes price prediction',
             'r2': five_mins_model_evaluation[0],
             'rmse': five_mins_model_evaluation[1],
             'mae': five_mins_model_evaluation[2]}]

    evaluation_df = ps_pd.DataFrame(data)

    print("Evaluate model: ")

    print(evaluation_df)

    try:
        one_min_model.write().overwrite().save("Model/oneMinuteModel/")

        print("One minute model is saved sucessfully!")
    except Exception as e:
        print(f"Error while saving one minute model:{e}")

    try:
        five_mins_model.write().overwrite().save("Model/fiveMinutesModel/")

        print("Five minutes model is saved sucessfully!")
    except Exception as e:
        print(f"Error while saving five minutes model:{e}")

    # Stop spark session
    spark.stop()


if __name__ == "__main__":
    main()
