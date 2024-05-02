import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("linear regression test").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stock_data = spark.read.csv('stock_data.csv', header=True, inferSchema=True)

# Preprocess timestamp features
stock_data = stock_data.withColumn("hour", hour(col("time"))) \
                        .withColumn("minute", minute(col("time")))

# Create StringIndexer for column 'ticker'
# string_indexer = StringIndexer(inputCol="ticker", outputCol="ticker_index")
#
# stock_data = string_indexer.fit(stock_data).transform(stock_data)
#
# onehot_encoder = OneHotEncoder(inputCols=["ticker_index"], outputCols=["ticker_encoded"])
#
# stock_data = onehot_encoder.fit(stock_data).transform(stock_data)

assembler = VectorAssembler(inputCols=[
    'hour', 'minute', 'price', 'volume',
    'last_one_minute_price', 'last_one_minute_volume', 'last_two_minutes_price',
    'last_two_minutes_volume', 'last_three_minutes_price', 'last_three_minutes_volume',
    'last_four_minutes_price', 'last_four_minutes_volume', 'last_five_minutes_price',
    'last_five_minutes_volume'],
    outputCol='features')

stock_transformed_data = assembler.transform(stock_data)

(training_data, testing_data) = stock_transformed_data.randomSplit([0.8, 0.2], seed=42)

one_min_lr = LinearRegression(featuresCol="features", labelCol="next_one_minute_price")

five_mins_lr = LinearRegression(featuresCol="features", labelCol="next_five_minutes_price")

one_min_model = one_min_lr.fit(training_data)

five_mins_model = five_mins_lr.fit(training_data)

"""
    Evaluate the model
"""

one_min_prediction = one_min_model.transform(testing_data)

five_mins_prediction = five_mins_model.transform(testing_data)

"""
    For predicting stock index in next one minute
"""

one_min_evaluator_r2 = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction", metricName="r2")

one_min_r2 = one_min_evaluator_r2.evaluate(one_min_prediction)

one_min_evaluator_rmse = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction", metricName="rmse")

one_min_rmse = one_min_evaluator_rmse.evaluate(one_min_prediction)

one_min_evaluator_mae = RegressionEvaluator(labelCol="next_one_minute_price", predictionCol="prediction", metricName="mae")

one_min_mae = one_min_evaluator_mae.evaluate(one_min_prediction)

"""
    For predicting stock index in next one minute
"""

five_mins_evaluator_r2 = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction", metricName="r2")

five_mins_r2 = five_mins_evaluator_r2.evaluate(five_mins_prediction)

five_mins_evaluator_rmse = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction", metricName="rmse")

five_mins_rmse = five_mins_evaluator_rmse.evaluate(five_mins_prediction)

five_mins_evaluator_mae = RegressionEvaluator(labelCol="next_five_minutes_price", predictionCol="prediction", metricName="mae")

five_mins_mae = five_mins_evaluator_mae.evaluate(five_mins_prediction)

data = [{'model': 'next one minute price prediction', 'r2': one_min_r2, 'rmse': one_min_rmse, 'mae': one_min_mae},
        {'model': 'next five minutes price prediction', 'r2': five_mins_r2, 'rmse': five_mins_rmse,
         'mae': five_mins_mae}]

# evaluation_df = pd.DataFrame(data)

# try:
#     evaluation_df.to_csv('model_evaluation.csv')
#
#     print("Export successfully!")
# except Exception as e:
#     print(f"Error while exporting csv file: {e}")

# print(stock_data.head(2))

# one_min_prediction.select("features", "next_one_minute_price", "prediction").show(5)
#
# five_mins_prediction.select("features", "next_five_minutes_price", "prediction").show(5)

print(testing_data.dtypes)
